package ds

import (
	"github.com/sqlvibe/sqlvibe/internal/pb"
)

type PageManager struct {
	file     pb.File
	pageSize int
	numPages uint32
	header   *DatabaseHeader
	freeList []uint32
}

func NewPageManager(file pb.File, pageSize int) (*PageManager, error) {
	pm := &PageManager{
		file:     file,
		pageSize: pageSize,
		numPages: 0,
		freeList: make([]uint32, 0),
	}

	size, err := file.Size()
	if err != nil {
		return nil, err
	}

	if size == 0 {
		pm.header = NewDatabaseHeader(uint16(pageSize))
		if err := pm.writeHeader(); err != nil {
			return nil, err
		}
		pm.numPages = 1
	} else {
		pm.numPages = uint32(size / int64(pageSize))
		if err := pm.readHeader(); err != nil {
			return nil, err
		}
	}

	return pm, nil
}

func (pm *PageManager) PageSize() int {
	return pm.pageSize
}

func (pm *PageManager) NumPages() uint32 {
	return pm.numPages
}

func (pm *PageManager) Header() *DatabaseHeader {
	return pm.header
}

func (pm *PageManager) ReadPage(pageNum uint32) (*Page, error) {
	if pageNum == 0 || pageNum > pm.numPages {
		return nil, ErrInvalidPage
	}

	page := NewPage(pageNum, pm.pageSize)
	offset := int64(pageNum-1) * int64(pm.pageSize)

	_, err := pm.file.ReadAt(page.Data, offset)
	if err != nil {
		return nil, err
	}

	if pageNum == 1 {
		header, err := ParseHeader(page.Data)
		if err != nil {
			return nil, err
		}
		pm.header = header
		page.Type = PageType(0)
	} else {
		page.Type = PageType(page.Data[0])
	}

	return page, nil
}

func (pm *PageManager) WritePage(page *Page) error {
	offset := int64(page.Num-1) * int64(pm.pageSize)
	_, err := pm.file.WriteAt(page.Data, offset)
	if err != nil {
		return err
	}
	page.IsDirty = false
	return nil
}

func (pm *PageManager) AllocatePage() (uint32, error) {
	if len(pm.freeList) > 0 {
		pageNum := pm.freeList[len(pm.freeList)-1]
		pm.freeList = pm.freeList[:len(pm.freeList)-1]
		return pageNum, nil
	}

	pm.numPages++
	pageNum := pm.numPages
	pm.header.DatabaseSize = pm.numPages
	pm.header.FileChangeCounter++

	newSize := int64(pm.numPages) * int64(pm.pageSize)
	if err := pm.file.Truncate(newSize); err != nil {
		return 0, err
	}

	if err := pm.writeHeader(); err != nil {
		return 0, err
	}

	return pageNum, nil
}

func (pm *PageManager) FreePage(pageNum uint32) error {
	if pageNum == 0 || pageNum > pm.numPages {
		return ErrInvalidPage
	}
	pm.freeList = append(pm.freeList, pageNum)
	return nil
}

func (pm *PageManager) Sync() error {
	return pm.file.Sync()
}

func (pm *PageManager) Close() error {
	if err := pm.file.Sync(); err != nil {
		return err
	}
	return pm.file.Close()
}

func (pm *PageManager) readHeader() error {
	headerData := make([]byte, pm.pageSize)
	_, err := pm.file.ReadAt(headerData, 0)
	if err != nil {
		return err
	}
	pm.header, err = ParseHeader(headerData)
	return err
}

func (pm *PageManager) writeHeader() error {
	headerData := make([]byte, 100)
	if err := pm.header.WriteTo(headerData); err != nil {
		return err
	}
	_, err := pm.file.WriteAt(headerData, 0)
	return err
}
