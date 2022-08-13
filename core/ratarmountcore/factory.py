#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import sys
import traceback

from typing import IO, Union

from .compressions import (
    ARCHIVE_FORMATS,
    TAR_COMPRESSION_FORMATS,
    supportedCompressions,
    checkForSplitFile,
    rarfile,
    zipfile,
    libarchive,
)
from .utils import CompressionError, RatarmountError
from .MountSource import MountSource
from .FolderMountSource import FolderMountSource
from .RarMountSource import RarMountSource
from .SingleFileMountSource import SingleFileMountSource
from .SQLiteIndexedTar import SQLiteIndexedTar
from .StenciledFile import JoinedFileFromFactory
from .ZipMountSource import ZipMountSource
from .LibarchiveMountSource import LibarchiveMountSource


def openMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> MountSource:
    printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

    joinedFileName = ''
    if isinstance(fileOrPath, str):
        if not os.path.exists(fileOrPath):
            raise RatarmountError(f"Mount source does not exist: {fileOrPath}")

        if os.path.isdir(fileOrPath):
            return FolderMountSource('.' if fileOrPath == '.' else os.path.realpath(fileOrPath))

        splitFileResult = checkForSplitFile(fileOrPath)
        if splitFileResult:
            filesToJoin = splitFileResult[0]
            joinedFileName = os.path.basename(filesToJoin[0]).rsplit('.', maxsplit=1)[0]
            if 'indexFilePath' not in options or not options['indexFilePath']:
                options['indexFilePath'] = filesToJoin[0] + ".index.sqlite"
            # https://docs.python.org/3/faq/programming.html
            # > Why do lambdas defined in a loop with different values all return the same result?
            fileOrPath = JoinedFileFromFactory(
                [(lambda file=file: open(file, 'rb')) for file in filesToJoin]  # type: ignore
            )

    if not isinstance(fileOrPath, str):
        print("READ FIRST TWO BYTES:", fileOrPath.read(2))
        # TODO SEEKING BACK DOES NOT WORK! It will simply read the next two bytes and even throw
        # when the end of file has been reached!!!
        fileOrPath.seek(0, io.SEEK_SET)
        print("READ FIRST TWO BYTES:", fileOrPath.read(2))

    if "libarchive" in sys.modules:
        # Neither python-libarchive nor libarchive support opening raw Python file objects.
        # Because of the latter it might not be possible anytime soon. Test against this for better error messages.
        hasFileNumber = isinstance(fileOrPath, str)
        if not isinstance(fileOrPath, str) and hasattr(fileOrPath, 'fileno'):
            try:
                fileOrPath.fileno()
                hasFileNumber = True
            except io.UnsupportedOperation:
                pass

        if hasFileNumber:
            forceLibarchive: bool = bool(options.get("forceLibarchive", False))
            if printDebug > 1 and forceLibarchive:
                print("[Info] ZIP, TAR, and RAR files will be handled by libarchive instead of the default backends.")

            try:
                # libarchive.is_archive checks whether it is the archive is any of the types specified in "formats"
                # optionally compressed with any of the compression standards specified in "filters", i.e., "filters"
                # does not filter archive formats! If any of both is empty, it is interpreted as wildcards.
                # E.g., use is_archive(path, formats=('tar',), filter=('bz2')) to only test for .tar.bz2 files.
                # When not forced, do not open any of the formats for which a backend exists.
                archiveIsEligible = (
                    libarchive.is_archive(fileOrPath)
                    if forceLibarchive
                    else not libarchive.is_archive(
                        fileOrPath,
                        formats=['tar'] + list(ARCHIVE_FORMATS.keys()),
                        filters=tuple(TAR_COMPRESSION_FORMATS.keys()),
                    )
                )

                if archiveIsEligible:
                    if printDebug >= 1:
                        print("[Info] Opening archive with libarchive backend.")
                        print("[Info] No index will be created and performance is untestest.")
                    return LibarchiveMountSource(fileOrPath, **options)
            except Exception as exception:
                if printDebug >= 1:
                    print("[Info] Checking for libarchive file raised an exception:", exception)
                if printDebug >= 2:
                    traceback.print_exc()
            finally:
                try:
                    if hasattr(fileOrPath, 'seek'):
                        fileOrPath.seek(0)  # type: ignore
                except Exception as exception:
                    if printDebug >= 1:
                        print("[Info] seek(0) raised an exception:", exception)
                    if printDebug >= 2:
                        traceback.print_exc()

    try:
        if 'rarfile' in sys.modules and rarfile.is_rarfile(fileOrPath):
            return RarMountSource(fileOrPath, **options)
    except Exception as exception:
        if printDebug >= 1:
            print("[Info] Checking for RAR file raised an exception:", exception)
        if printDebug >= 2:
            traceback.print_exc()
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore

    try:
        if isinstance(fileOrPath, str):
            return SQLiteIndexedTar(fileOrPath, **options)

        return SQLiteIndexedTar(fileObject=fileOrPath, **options)
    except RatarmountError as exception:
        if printDebug >= 2:
            print("[Info] Checking for (compressed) TAR file raised an exception:", exception)
        if printDebug >= 3:
            traceback.print_exc()
    except Exception as exception:
        if printDebug >= 1:
            print("[Info] Checking for (compressed) TAR file raised an exception:", exception)
        if printDebug >= 3:
            traceback.print_exc()
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore

    if 'zipfile' in sys.modules and zipfile is not None:
        try:
            # is_zipfile is much too lax when testing for ZIPs because it's only testing for the central directory
            # at the end of the file not the magic bits at the beginning. Meaning, if another non-ZIP archive has
            # zip contents at the end, then it might get misclassified! Thefore, manually check for PK at start.
            # https://bugs.python.org/issue16735
            # https://bugs.python.org/issue28494
            # https://bugs.python.org/issue42096
            # https://bugs.python.org/issue45287
            # TODO This will not recognize self-extracting ZIP archives, so for now, those are simply not supported!
            if isinstance(fileOrPath, str):
                with open(fileOrPath, 'rb') as file:
                    if supportedCompressions['zip'].checkHeader(file) and zipfile.is_zipfile(fileOrPath):
                        return ZipMountSource(fileOrPath, **options)
            else:
                # TODO One problem here is when trying to read and then seek back but there also is no peek method.
                #      https://github.com/markokr/rarfile/issues/73
                if fileOrPath.read(2) == b'PK' and zipfile.is_zipfile(fileOrPath):
                    return ZipMountSource(fileOrPath, **options)
        except Exception as exception:
            if printDebug >= 1:
                print("[Info] Checking for ZIP file raised an exception:", exception)
            if printDebug >= 3:
                traceback.print_exc()
        finally:
            if hasattr(fileOrPath, 'seek'):
                fileOrPath.seek(0)  # type: ignore

    if joinedFileName and not isinstance(fileOrPath, str):
        return SingleFileMountSource(joinedFileName, fileOrPath)

    raise CompressionError(f"Archive to open ({str(fileOrPath)}) has unrecognized format!")
