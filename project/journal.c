#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

/* ========= Constants from mkfs.c ========= */

#define BLOCK_SIZE        4096U  // Size of each block in bytes

#define JOURNAL_BLOCK_IDX 1U     // Journal starts at block 1
#define JOURNAL_BLOCKS    16U    // Journal occupies 16 blocks

// Bitmap and inode table positions
#define INODE_BMAP_IDX    (JOURNAL_BLOCK_IDX + JOURNAL_BLOCKS)  // Block 17
#define DATA_BMAP_IDX     (INODE_BMAP_IDX + 1U)                 // Block 18
#define INODE_START_IDX   (DATA_BMAP_IDX + 1U)                  // Block 19
#define INODE_BLOCKS      2U                                     // 2 blocks for inodes
#define DATA_START_IDX    (INODE_START_IDX + INODE_BLOCKS)      // Block 21

#define MAX_NAME 28       // Max filename length

#define JTYPE_DATA   1    // Journal record type: DATA
#define JTYPE_COMMIT 2    // Journal record type: COMMIT

/* ========= On-disk structures ========= */

typedef struct {
    uint32_t inode_table_start;  // Starting block of inode table
} superblock_t;

typedef struct {
    uint16_t type;      // 0=free, 1=file, 2=dir
    uint16_t links;     // Number of hard links
    uint32_t size;      // File size in bytes
    uint32_t blocks[12]; // Direct block pointers
} inode_t;

typedef struct {
    uint32_t inode;     // Inode number
    char name[MAX_NAME]; // Filename
} dirent_t;

typedef struct {
    uint32_t type;      // Record type (JTYPE_DATA)
    uint32_t block_no;  // Which block to write
    uint8_t  data[BLOCK_SIZE];  // Full block contents
} journal_data_t;

typedef struct {
    uint32_t type;      // Record type (JTYPE_COMMIT)
} journal_commit_t;

typedef struct {
    uint32_t nbytes_used;  // How many bytes of journal are used
} journal_header_t;

/* ========= Low-level I/O ========= */

static off_t blkoff(uint32_t b) {
    return (off_t)b * BLOCK_SIZE;  // Convert block number to byte offset
}

static void readblk(int fd, uint32_t b, void *buf) {
    lseek(fd, blkoff(b), SEEK_SET);  // Seek to block position
    read(fd, buf, BLOCK_SIZE);        // Read full block
}

static void writeblk(int fd, uint32_t b, void *buf) {
    lseek(fd, blkoff(b), SEEK_SET);  // Seek to block position
    write(fd, buf, BLOCK_SIZE);       // Write full block
}

/* ========= Journal helpers ========= */

static void read_jhdr(int fd, journal_header_t *h) {
    readblk(fd, JOURNAL_BLOCK_IDX, h);  // Read journal header from block 1
}

static void write_jhdr(int fd, journal_header_t *h) {
    writeblk(fd, JOURNAL_BLOCK_IDX, h);  // Write journal header to block 1
}

/* ========= CREATE ========= */

static void cmd_create(const char *img, const char *name) {
    int fd = open(img, O_RDWR);
    if (fd < 0) {
        perror("open");
        exit(1);
    }

    superblock_t sb;
    readblk(fd, 0, &sb);

    journal_header_t jh;
    read_jhdr(fd, &jh);

    // Calculate transaction size: 3 DATA records + 1 COMMIT
    size_t txn_size =
        3 * sizeof(journal_data_t) + sizeof(journal_commit_t);

    // Check if journal has enough space
    size_t journal_cap =
        JOURNAL_BLOCKS * BLOCK_SIZE - sizeof(journal_header_t);

    if (jh.nbytes_used + txn_size > journal_cap) {
        fprintf(stderr, "journal full\n");
        exit(1);
    }

    /* ---- read current metadata ---- */

    uint8_t inode_bmap[BLOCK_SIZE];  // Inode bitmap
    uint8_t itable_blk[BLOCK_SIZE];  // Inode table block
    uint8_t dirblk[BLOCK_SIZE];      // Root directory block

    readblk(fd, INODE_BMAP_IDX, inode_bmap);  // Read inode bitmap
    readblk(fd, DATA_START_IDX, dirblk);      // Read root directory

    /* ---- allocate inode ---- */

    int ino = -1;
    // Scan bitmap to find first free inode
    for (int i = 0; i < BLOCK_SIZE * 8; i++) {
        if (!(inode_bmap[i / 8] & (1 << (i % 8)))) {  // Bit not set = free
            inode_bmap[i / 8] |= (1 << (i % 8));      // Mark as used
            ino = i;
            break;
        }
    }
    if (ino < 0)
        exit(1);

    /* ---- inode table block calculation ---- */

    int ipb = BLOCK_SIZE / sizeof(inode_t);  // Inodes per block
    uint32_t itable_block =
        INODE_START_IDX + (ino / ipb);  // Which block contains this inode
    uint32_t itable_off = ino % ipb;     // Offset within that block

    readblk(fd, itable_block, itable_blk);  // Read inode table block

    inode_t *inodes = (inode_t *)itable_blk;
    inode_t *in = &inodes[itable_off];  // Pointer to our new inode

    // Initialize new inode
    memset(in, 0, sizeof(*in));
    in->type = 1;      // regular file
    in->links = 1;     // one link
    in->size = 0;      // empty file

    /* ---- add directory entry ---- */

    dirent_t *ents = (dirent_t *)dirblk;
    // Find first free slot (skip . and .. at index 0 and 1)
    for (int i = 2; i < BLOCK_SIZE / sizeof(dirent_t); i++) {
        if (ents[i].inode == 0) {
            ents[i].inode = ino;
            strncpy(ents[i].name, name, MAX_NAME - 1);
            break;
        }
    }

    /* ---- append journal records ---- */

    // Calculate where to write in journal
    off_t off =
        blkoff(JOURNAL_BLOCK_IDX) +
        sizeof(journal_header_t) +
        jh.nbytes_used;

    journal_data_t d;

    // Write DATA record for inode bitmap
    d.type = JTYPE_DATA;
    d.block_no = INODE_BMAP_IDX;
    memcpy(d.data, inode_bmap, BLOCK_SIZE);
    pwrite(fd, &d, sizeof(d), off);
    off += sizeof(d);

    // Write DATA record for inode table
    d.block_no = itable_block;
    memcpy(d.data, itable_blk, BLOCK_SIZE);
    pwrite(fd, &d, sizeof(d), off);
    off += sizeof(d);

    // Write DATA record for root directory
    d.block_no = DATA_START_IDX;
    memcpy(d.data, dirblk, BLOCK_SIZE);
    pwrite(fd, &d, sizeof(d), off);
    off += sizeof(d);

    // Write COMMIT record to seal transaction
    journal_commit_t c = { JTYPE_COMMIT };
    pwrite(fd, &c, sizeof(c), off);

    // Update journal header with new size
    jh.nbytes_used += txn_size;
    write_jhdr(fd, &jh);

    printf("Logged creation of %s to journal.\n", name);

    close(fd);
}

/* ========= INSTALL ========= */

static void cmd_install(const char *img) {
    int fd = open(img, O_RDWR);  // Open disk image
    if (fd < 0) {
        perror("open");
        exit(1);
    }

    journal_header_t jh;
    read_jhdr(fd, &jh);  // Read journal header

    // Start reading after journal header
    off_t off =
        blkoff(JOURNAL_BLOCK_IDX) +
        sizeof(journal_header_t);

    off_t end = off + jh.nbytes_used;  // End of valid journal data

    journal_data_t pending[8];  // Buffer for DATA records
    int np = 0;  // Number of pending records

    // Parse journal records
    while (off < end) {
        uint32_t type;
        pread(fd, &type, sizeof(type), off);  // Read record type

        if (type == JTYPE_DATA) {
            // Store DATA record
            pread(fd, &pending[np], sizeof(journal_data_t), off);
            np++;
            off += sizeof(journal_data_t);
        } else if (type == JTYPE_COMMIT) {
            // COMMIT found: apply all pending DATA records
            for (int i = 0; i < np; i++)
                writeblk(fd,
                         pending[i].block_no,  // Write to home location
                         pending[i].data);
            np = 0;  // Reset pending count
            off += sizeof(journal_commit_t);
        } else {
            break;  // Unknown type or incomplete txn
        }
    }

    /* clear journal */
    jh.nbytes_used = 0;  // Reset journal
    write_jhdr(fd, &jh);

    printf("Journal installed\n");

    close(fd);
}

/* ========= MAIN ========= */

int main(int argc, char *argv[]) {
    // Check if at least one argument provided
    if (argc < 2) {
        fprintf(stderr, "Usage: ./journal create <name> | install\n");
        return 1;
    }

    // Handle "create" command
    if (!strcmp(argv[1], "create")) {
        if (argc < 3) {  // Need filename argument
            fprintf(stderr, "Usage: ./journal create <name>\n");
            return 1;
        }
        cmd_create("vsfs.img", argv[2]);  // Image hardcoded, argv[2] = filename
    }
    // Handle "install" command
    else if (!strcmp(argv[1], "install")) {
        cmd_install("vsfs.img");  // Image hardcoded, no extra args
    }
    // Unknown command
    else {
        fprintf(stderr, "Usage: ./journal create <name> | install\n");
        return 1;
    }

    return 0;
}
