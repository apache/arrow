//
// The MIT License (MIT)
//
// Copyright (c) 2016 Alexander Kormanovsky
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include "date/ios.h"

#if TARGET_OS_IPHONE

#include <Foundation/Foundation.h>

#include <fstream>
#include <zlib.h>
#include <sys/stat.h>

#ifndef TAR_DEBUG
#  define TAR_DEBUG 0
#endif

#define INTERNAL_DIR        "Library"
#define TZDATA_DIR          "tzdata"
#define TARGZ_EXTENSION     "tar.gz"

#define TAR_BLOCK_SIZE                  512
#define TAR_TYPE_POSITION               156
#define TAR_NAME_POSITION               0
#define TAR_NAME_SIZE                   100
#define TAR_SIZE_POSITION               124
#define TAR_SIZE_SIZE                   12

namespace date
{
    namespace iOSUtils
    {

        struct TarInfo
        {
            char objType;
            std::string objName;
            size_t realContentSize; // writable size without padding zeroes
            size_t blocksContentSize; // adjusted size to 512 bytes blocks
            bool success;
        };

        std::string convertCFStringRefPathToCStringPath(CFStringRef ref);
        bool extractTzdata(CFURLRef homeUrl, CFURLRef archiveUrl, std::string destPath);
        TarInfo getTarObjectInfo(std::ifstream &readStream);
        std::string getTarObject(std::ifstream &readStream, int64_t size);
        bool writeFile(const std::string &tzdataPath, const std::string &fileName,
                       const std::string &data, size_t realContentSize);

        std::string
        get_current_timezone()
        {
            CFTimeZoneRef tzRef = CFTimeZoneCopySystem();
            CFStringRef tzNameRef = CFTimeZoneGetName(tzRef);
            CFIndex bufferSize = CFStringGetLength(tzNameRef) + 1;
            char buffer[bufferSize];

            if (CFStringGetCString(tzNameRef, buffer, bufferSize, kCFStringEncodingUTF8))
            {
                CFRelease(tzRef);
                return std::string(buffer);
            }

            CFRelease(tzRef);

            return "";
        }

        std::string
        get_tzdata_path()
        {
            CFURLRef homeUrlRef = CFCopyHomeDirectoryURL();
            CFStringRef homePath = CFURLCopyPath(homeUrlRef);
            std::string path(std::string(convertCFStringRefPathToCStringPath(homePath)) +
                             INTERNAL_DIR + "/" + TZDATA_DIR);
            std::string result_path(std::string(convertCFStringRefPathToCStringPath(homePath)) +
                                    INTERNAL_DIR);

            if (access(path.c_str(), F_OK) == 0)
            {
#if TAR_DEBUG
                printf("tzdata dir exists\n");
#endif
                CFRelease(homeUrlRef);
                CFRelease(homePath);

                return result_path;
            }

            CFBundleRef mainBundle = CFBundleGetMainBundle();
            CFArrayRef paths = CFBundleCopyResourceURLsOfType(mainBundle, CFSTR(TARGZ_EXTENSION),
                                                              NULL);

            if (CFArrayGetCount(paths) != 0)
            {
                // get archive path, assume there is no other tar.gz in bundle
                CFURLRef archiveUrl = static_cast<CFURLRef>(CFArrayGetValueAtIndex(paths, 0));
                CFStringRef archiveName = CFURLCopyPath(archiveUrl);
                archiveUrl = CFBundleCopyResourceURL(mainBundle, archiveName, NULL, NULL);

                extractTzdata(homeUrlRef, archiveUrl, path);

                CFRelease(archiveUrl);
                CFRelease(archiveName);
            }

            CFRelease(homeUrlRef);
            CFRelease(homePath);
            CFRelease(paths);

            return result_path;
        }

        std::string
        convertCFStringRefPathToCStringPath(CFStringRef ref)
        {
            CFIndex bufferSize = CFStringGetMaximumSizeOfFileSystemRepresentation(ref);
            char *buffer = new char[bufferSize];
            CFStringGetFileSystemRepresentation(ref, buffer, bufferSize);
            auto result = std::string(buffer);
            delete[] buffer;
            return result;
        }

        bool
        extractTzdata(CFURLRef homeUrl, CFURLRef archiveUrl, std::string destPath)
        {
            std::string TAR_TMP_PATH = "/tmp.tar";

            CFStringRef homeStringRef = CFURLCopyPath(homeUrl);
            auto homePath = convertCFStringRefPathToCStringPath(homeStringRef);
            CFRelease(homeStringRef);

            CFStringRef archiveStringRef = CFURLCopyPath(archiveUrl);
            auto archivePath = convertCFStringRefPathToCStringPath(archiveStringRef);
            CFRelease(archiveStringRef);

            // create Library path
            auto libraryPath = homePath + INTERNAL_DIR;

            // create tzdata path
            auto tzdataPath = libraryPath + "/" + TZDATA_DIR;

            // -- replace %20 with " "
            const std::string search = "%20";
            const std::string replacement = " ";
            size_t pos = 0;

            while ((pos = archivePath.find(search, pos)) != std::string::npos) {
                archivePath.replace(pos, search.length(), replacement);
                pos += replacement.length();
            }

            gzFile tarFile = gzopen(archivePath.c_str(), "rb");

            // create tar unpacking path
            auto tarPath = libraryPath + TAR_TMP_PATH;

            // create tzdata directory
            mkdir(destPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

            // ======= extract tar ========

            std::ofstream os(tarPath.c_str(), std::ofstream::out | std::ofstream::app);
            unsigned int bufferLength = 1024 * 256;  // 256Kb
            unsigned char *buffer = (unsigned char *)malloc(bufferLength);
            bool success = true;

            while (true)
            {
                int readBytes = gzread(tarFile, buffer, bufferLength);

                if (readBytes > 0)
                {
                    os.write((char *) &buffer[0], readBytes);
                }
                else
                    if (readBytes == 0)
                    {
                        break;
                    }
                    else
                        if (readBytes == -1)
                        {
                            printf("decompression failed\n");
                            success = false;
                            break;
                        }
                        else
                        {
                            printf("unexpected zlib state\n");
                            success = false;
                            break;
                        }
            }

            os.close();
            free(buffer);
            gzclose(tarFile);

            if (!success)
            {
                remove(tarPath.c_str());
                return false;
            }

            // ======== extract files =========

            uint64_t location = 0; // Position in the file

            // get file size
            struct stat stat_buf;
            int res = stat(tarPath.c_str(), &stat_buf);
            if (res != 0)
            {
                printf("error file size\n");
                remove(tarPath.c_str());
                return false;
            }
            int64_t tarSize = stat_buf.st_size;

            // create read stream
            std::ifstream is(tarPath.c_str(), std::ifstream::in | std::ifstream::binary);

            // process files
            while (location < tarSize)
            {
                TarInfo info = getTarObjectInfo(is);

                if (!info.success || info.realContentSize == 0)
                {
                    break; // something wrong or all files are read
                }

                switch (info.objType)
                {
                    case '0':   // file
                    case '\0':  //
                    {
                        std::string obj = getTarObject(is, info.blocksContentSize);
#if TAR_DEBUG
                        size += info.realContentSize;
                        printf("#%i %s file size %lld written total %ld from %lld\n", ++count,
                               info.objName.c_str(), info.realContentSize, size, tarSize);
#endif
                        writeFile(tzdataPath, info.objName, obj, info.realContentSize);
                        location += info.blocksContentSize;

                        break;
                    }
                }
            }

            remove(tarPath.c_str());

            return true;
        }

        TarInfo
        getTarObjectInfo(std::ifstream &readStream)
        {
            int64_t length = TAR_BLOCK_SIZE;
            char buffer[length];
            char type;
            char name[TAR_NAME_SIZE + 1];
            char sizeBuf[TAR_SIZE_SIZE + 1];

            readStream.read(buffer, length);

            memcpy(&type, &buffer[TAR_TYPE_POSITION], 1);

            memset(&name, '\0', TAR_NAME_SIZE + 1);
            memcpy(&name, &buffer[TAR_NAME_POSITION], TAR_NAME_SIZE);

            memset(&sizeBuf, '\0', TAR_SIZE_SIZE + 1);
            memcpy(&sizeBuf, &buffer[TAR_SIZE_POSITION], TAR_SIZE_SIZE);
            size_t realSize = strtol(sizeBuf, NULL, 8);
            size_t blocksSize = realSize + (TAR_BLOCK_SIZE - (realSize % TAR_BLOCK_SIZE));

            return {type, std::string(name), realSize, blocksSize, true};
        }

        std::string
        getTarObject(std::ifstream &readStream, int64_t size)
        {
            char buffer[size];
            readStream.read(buffer, size);
            return std::string(buffer);
        }

        bool
        writeFile(const std::string &tzdataPath, const std::string &fileName, const std::string &data,
                  size_t realContentSize)
        {
            std::ofstream os(tzdataPath + "/" + fileName, std::ofstream::out | std::ofstream::binary);

            if (!os) {
                return false;
            }

            // trim empty space
            char trimmedData[realContentSize + 1];
            memset(&trimmedData, '\0', realContentSize);
            memcpy(&trimmedData, data.c_str(), realContentSize);

            // write
            os.write(trimmedData, realContentSize);
            os.close();

            return true;
        }

    }  // namespace iOSUtils
}  // namespace date

#endif  // TARGET_OS_IPHONE
