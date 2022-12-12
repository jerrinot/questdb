/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include "sysutil.h"

#include <stdlib.h>
#include <dirent.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/mount.h>
#include "files.h"

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_write
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {

    off_t writeOffset = offset;
    ssize_t written;

    do {
        size_t count = len > MAX_RW_COUNT ? MAX_RW_COUNT : len;
        RESTARTABLE(pwrite((int) fd, (void *) (address), count, writeOffset), written);
        if (written < 0) {
            return written;
        }
        len -= written;
        writeOffset += written;
        address += written;
        // Exit if written == 0 or there is nothing to write
    } while (len > 0 && written > 0);

    return writeOffset - offset;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mmap0
        (JNIEnv *e, jclass cl, jint fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
    int prot = 0;

    if (flags == com_questdb_std_Files_MAP_RO) {
        prot = PROT_READ;
    } else if (flags == com_questdb_std_Files_MAP_RW) {
        prot = PROT_READ | PROT_WRITE;
    }
    return (jlong) mmap((void *) baseAddress, (size_t) len, prot, MAP_SHARED, (int) fd, offset);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_munmap0
        (JNIEnv *cl, jclass e, jlong address, jlong len) {
    return munmap((void *) address, (size_t) len);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_append
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len) {
    ssize_t res;
    RESTARTABLE(write((int) fd, (void *) (address), (size_t) len), res);
    return res;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_read
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {

    off_t readOffset = offset;
    ssize_t read;

    do {
        size_t count = len > MAX_RW_COUNT ? MAX_RW_COUNT : len;
        RESTARTABLE(pread((int) fd, (void *) (address), count, readOffset), read);
        if (read < 0) {
            return read;
        }
        len -= read;
        readOffset += read;
        address += read;

        // Exit if read the given length or EOL (read == 0)
    } while (len > 0 && read > 0);

    return readOffset - offset;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_readNonNegativeInt
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    jint result;
    ssize_t readLen = pread((int) fd, (void *) &result, (size_t) 4, (off_t) offset);
    if (readLen != 4) {
        return -1;
    }
    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readNonNegativeLong
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    jlong result;
    ssize_t readLen;
    RESTARTABLE(pread((int) fd, (void *) &result, (size_t) 8, (off_t) offset), readLen);
    if (readLen != 8) {
        return -1;
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open((const char *) lpszName, O_RDONLY);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    return close((int) fd);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_RDWR, 0644);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRWOpts
        (JNIEnv *e, jclass cl, jlong lpszName, jlong opts) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_RDWR | opts, 0644);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_WRONLY | O_APPEND, 0644);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length0
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_hardLink
        (JNIEnv *e, jclass cl, jlong pcharSrc, jlong pcharHardLink) {
    return link((const char *) pcharSrc, (const char *) pcharHardLink);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_isSoftLink
        (JNIEnv *e, jclass cl, jlong pcharSoftLink) {

    struct stat st;
    if (lstat((const char *) pcharSoftLink, &st) == 0) {
        return S_ISLNK(st.st_mode);
    }
    return JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_softLink
        (JNIEnv *e, jclass cl, jlong pcharSrc, jlong pcharSoftLink) {
    return symlink((const char *) pcharSrc, (const char *) pcharSoftLink);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_unlink
        (JNIEnv *e, jclass cl, jlong pcharSoftLink) {
    return unlink((const char *) pcharSoftLink);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_mkdir
        (JNIEnv *e, jclass cl, jlong pchar, jint mode) {
    return mkdir((const char *) pchar, (mode_t) mode);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jint fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jint fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return (jboolean) (r == 0 ? st.st_nlink > 0 : 0);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) 1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
    if (ftruncate((int) fd, len) == 0) {
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

#ifdef __APPLE__

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
    // MACOS allocates additional space. Check what size the file currently is
    struct stat st;
    int _fd = (int) fd;
    if (fstat(_fd, &st) != 0) {
        return JNI_FALSE;
    }
    const jlong fileLen = st.st_blksize * st.st_blocks;
    jlong deltaLen = len - fileLen;
    if (deltaLen > 0) {
        // F_ALLOCATECONTIG - try to allocate continuous space.
        fstore_t flags = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, deltaLen, 0};
        int result = fcntl(_fd, F_PREALLOCATE, &flags);
        if (result == -1) {
            // F_ALLOCATEALL - try to allocate non-continuous space.
            flags.fst_flags = F_ALLOCATEALL;
            result = fcntl((int) fd, F_PREALLOCATE, &flags);
            if (result == -1) {
                return JNI_FALSE;
            }
        }
    }
    return ftruncate((int) fd, len) == 0;
}

#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_msync(JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    return msync((void *) addr, len, async ? MS_ASYNC : MS_SYNC);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsync(JNIEnv *e, jclass cl, jint fd) {
    return fsync((int) fd);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_sync(JNIEnv *e, jclass cl) {
    sync();
    return 0;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_remove
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) (remove((const char *) lpsz) == 0);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_rmdir
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) (rmdir((const char *) lpsz) == 0);
}

typedef struct {
    DIR *dir;
    struct dirent *entry;
} FIND;

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    DIR *dir;
    struct dirent *entry;

    dir = opendir((const char *) lpszName);
    if (!dir) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    errno = 0;
    entry = readdir(dir);
    if (!entry) {
        if (errno == 0) {
            closedir(dir);
            return 0;
        }
        closedir(dir);
        return -1;

    }

    FIND *find = malloc(sizeof(FIND));
    find->dir = dir;
    find->entry = entry;
    return (jlong) find;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    return sysconf(_SC_PAGESIZE);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    errno = 0;
    find->entry = readdir(find->dir);
    if (find->entry != NULL) {
        return 1;
    }
    return errno == 0 ? 0 : -1;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    closedir(find->dir);
    free(find);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return (jlong) ((FIND *) findPtr)->entry->d_name;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->entry->d_type;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_findTypeIsSoftLink
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->entry->d_type == DT_LNK;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jint fd) {
    return flock((int) fd, LOCK_EX | LOCK_NB);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openCleanRW
        (JNIEnv *e, jclass cl, jlong lpszName, jlong size) {

    jint fd = open((const char *) lpszName, O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        // error opening / creating file
        return fd;
    }

    jlong fileSize = Java_io_questdb_std_Files_length(e, cl, fd);
    if (fileSize > 0) {
        if (flock((int) fd, LOCK_EX | LOCK_NB) == 0) {
            // truncate file to 0 byte
            if (ftruncate(fd, 0) == 0) {
                // allocate file to `size`
                if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                    // downgrade to shared lock
                    if (flock((int) fd, LOCK_SH) == 0) {
                        // success
                        return fd;
                    }
                }
            }
        } else {
            if (fileSize >= size || Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                // put a shared lock
                if (flock((int) fd, LOCK_SH) == 0) {
                    // success
                    return fd;
                }
            }
        }
    } else {
        // file size is already 0, no cleanup but allocate the file.
        if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE && flock((int) fd, LOCK_SH) == 0) {
            // success
            return fd;
        }
    }

    // Any non-happy path comes here.
    // Save errno before close.
    int errnoTmp = errno;
    close(fd);
    // Restore real errno
    errno = errnoTmp;
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_rename
        (JNIEnv *e, jclass cls, jlong lpszOld, jlong lpszNew) {
    int err = rename((const char *) lpszOld, (const char *) lpszNew);
    if (err != 0) {
        return errno == EXDEV ? FILES_RENAME_ERR_EXDEV : FILES_RENAME_ERR_OTHER;
    }
    return FILES_RENAME_ERR_OK;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists0
        (JNIEnv *e, jclass cls, jlong lpsz) {
    return access((const char *) lpsz, F_OK) == 0;
}
