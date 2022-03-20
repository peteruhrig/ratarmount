#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bisect
import io

from typing import IO, List, Optional, Tuple

from .utils import overrides, _DummyContext


class RawStenciledFile(io.RawIOBase):
    # For a reference implementation based on RawIOBase, see "class SocketIO(io.RawIOBase)" in:
    #   https://github.com/python/cpython/tree/main/Lib/socket.py#L662
    # or others implementations inside cpython:
    #   https://github.com/python/cpython/tree/main/Lib/_compression.py#L66
    # For the internals of RawIOBase and others, see:
    #   https://github.com/python/cpython/tree/main/Lib/_pyio.py#L619
    """A file abstraction layer giving a stenciled view to an underlying file."""

    def __init__(
        self,
        fileobj: Optional[IO] = None,
        stencils: Optional[List[Tuple[int, int]]] = None,
        fileobjLock=None,
        fileStencils: Optional[List[Tuple[IO, int, int]]] = None,
    ) -> None:
        """
        stencils: A list tuples specifying the offset and length of the underlying file to use.
                  The order of these tuples will be kept.
                  The offset must be non-negative and the size must be positive.
        fileobj: (deprecated) Only either fileobj and stencils or fileStencils may be specified
        stencils: (deprecated) Only either fileobj and stencils or fileStencils may be specified
        fileStencils: Contains a list of (file object, offset, size) tuples. The offset and size
                      can be seen as a cut-out of the file obejct. All cut-outs are joined
                      together in the order of the list. Note that multiple cut-outs into the
                      same file object may be given by simply specifying the file objet multiple
                      times in the list.

        Examples:
            stencil = [(5,7)]
                Makes a new 7B sized virtual file starting at offset 5 of fileobj.
            stencil = [(0,3),(5,3)]
                Make a new 6B sized virtual file containing bytes [0,1,2,5,6,7] of fileobj.
            stencil = [(0,3),(0,3)]
                Make a 6B size file containing the first 3B of fileobj twice concatenated together.
        """

        self.offset = 0
        self.fileobjLock = fileobjLock
        self.offsets: List[int] = []
        self.sizes: List[int] = []
        self.fileObjects: List[IO] = []

        # Convert stencils to internal format
        if fileStencils:
            if stencils or fileobj:
                raise ValueError(
                    "Either the deprecated combination of fileobj and stencils or the new interface using "
                    "fileStencils may be specified, not both!"
                )
            self.fileObjects, self.offsets, self.sizes = zip(*fileStencils)
        else:
            if stencils:
                if not fileobj:
                    raise ValueError("Stencils may not be specified without a valid file object!")

                self.offsets = [x[0] for x in stencils]
                self.sizes = [x[1] for x in stencils]
                self.fileObjects = [fileobj] * len(self.sizes)
            else:
                self.offsets = []
                self.fileObjects = []

        # Check whether values make sense
        for offset in self.offsets:
            assert offset >= 0
        for size in self.sizes:
            assert size >= 0

        # Filter out zero-sized regions (or else we would have to skip them inside 'readinto' in order to not
        # return an empty reply even though the end of file has not been reached yet!)
        selectedStencils = [i for i, size in enumerate(self.sizes) if size > 0]
        self.offsets = [self.offsets[i] for i in selectedStencils]
        self.sizes = [self.sizes[i] for i in selectedStencils]
        self.fileObjects = [self.fileObjects[i] for i in selectedStencils]

        # Check for readability
        for fileObject in self.fileObjects:
            if not fileObject.readable():
                raise ValueError("All file objects to be joined must be readable")

        # Calculate cumulative sizes
        self.cumsizes = [0]
        for size in self.sizes:
            self.cumsizes.append(self.cumsizes[-1] + size)

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek(0)

    def _findStencil(self, offset: int) -> int:
        """
        Return index to stencil where offset belongs to. E.g., for stencils [(3,5),(8,2)], offsets 0 to
        and including 4 will still be inside stencil (3,5), i.e., index 0 will be returned. For offset 6,
        index 1 would be returned because it now is in the second contiguous region / stencil.
        """
        # bisect_left( value ) gives an index for a lower range: value < x for all x in list[0:i]
        # Because value >= 0 and list starts with 0 we can therefore be sure that the returned i>0
        # Consider the stencils [(11,2),(22,2),(33,2)] -> cumsizes [0,2,4,6]. Seek to offset 2 should seek to 22.
        assert offset >= 0
        i = bisect.bisect_left(self.cumsizes, offset + 1) - 1
        assert i >= 0
        return i

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    @overrides(io.RawIOBase)
    def close(self) -> None:
        # Don't close the file objects given to us.
        pass

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return all(fobj.seekable() for fobj in self.fileObjects)

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        return len(readBytes)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.cumsizes[-1] - self.offset

        # This loop works in a kind of leapfrog fashion. On each even loop iteration it seeks to the next stencil
        # and on each odd iteration it reads the data and increments the offset inside the stencil!
        result = b''
        i = self._findStencil(self.offset)
        if i >= len(self.sizes):
            return result

        with self.fileobjLock if self.fileobjLock else _DummyContext():
            # Note that seek and read of the file object itself do not seem to check against this and
            # instead lead to a segmentation fault in the multithreading tests.
            if self.fileObjects[i].closed:
                raise ValueError("A closed file can't be read from!")

            offsetInsideStencil = self.offset - self.cumsizes[i]
            assert offsetInsideStencil >= 0
            assert offsetInsideStencil < self.sizes[i]
            self.fileObjects[i].seek(self.offsets[i] + offsetInsideStencil, io.SEEK_SET)

            # Read as much as requested or as much as the current contiguous region / stencil still contains
            readableSize = min(size, self.sizes[i] - (self.offset - self.cumsizes[i]))
            tmp = self.fileObjects[i].read(readableSize)
            self.offset += len(tmp)
            result += tmp

        return result

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.cumsizes[-1] + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        if self.offset < 0:
            raise ValueError("Trying to seek before the start of the file!")
        if self.offset >= self.cumsizes[-1]:
            return self.offset

        return self.offset

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.offset


class StenciledFile(io.BufferedReader):
    def __init__(
        self,
        fileobj: Optional[IO] = None,
        stencils: Optional[List[Tuple[int, int]]] = None,
        fileobjLock=None,
        fileStencils: Optional[List[Tuple[IO, int, int]]] = None,
    ) -> None:
        super().__init__(RawStenciledFile(fileobj, stencils, fileobjLock, fileStencils))


class JoinedFile(io.BufferedReader):
    def __init__(self, file_objects: List[IO], file_lock=None, buffer_size: int = io.DEFAULT_BUFFER_SIZE) -> None:
        sizes = [fobj.seek(0, io.SEEK_END) for fobj in file_objects]
        for fobj, size in zip(file_objects, sizes):
            if size is None:
                raise ValueError("Failed to query size of file object:", fobj)

        fileStencils = [(fobj, 0, size if size else 0) for fobj, size in zip(file_objects, sizes)]
        super().__init__(RawStenciledFile(fileStencils=fileStencils, fileobjLock=file_lock), buffer_size=buffer_size)
