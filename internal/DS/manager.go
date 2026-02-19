package DS

import (
	"github.com/sqlvibe/sqlvibe/internal/PB"
)

const DefaultCacheSize = 100

type PageManager struct {
	file     PB.File
	pageSize int
	numPages uint32
	header   *DatabaseHeader
	freeList []uint32
	cache    *Cache
}

func NewPageManager(file PB.File, pageSize int) (*PageManager, error) {
	pm := &PageManager{
		file:     file,
		pageSize: pageSize,
		numPages: 0,
		freeList: make([]uint32, 0),
		cache:    NewCache(DefaultCacheSize),
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

func NewPageManagerWithCache(file PB.File, pageSize int, cacheSize int) (*PageManager, error) {
	pm := &PageManager{
		file:     file,
		pageSize: pageSize,
		numPages: 0,
		freeList: make([]uint32, 0),
		cache:    NewCache(cacheSize),
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

	if cachedPage, ok := pm.cache.Get(pageNum); ok {
		return cachedPage, nil
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

	pm.cache.Set(page)
	return page, nil
}

func (pm *PageManager) WritePage(page *Page) error {
	offset := int64(page.Num-1) * int64(pm.pageSize)
	_, err := pm.file.WriteAt(page.Data, offset)
	if err != nil {
		return err
	}
	page.IsDirty = false
	pm.cache.SetDirty(page.Num, false)
	return nil
}

func (pm *PageManager) AllocatePage() (uint32, error) {
	if len(pm.freeList) > 0 {
		pageNum := pm.freeList[len(pm.freeList)-1]
		pm.freeList = pm.freeList[:len(pm.freeList)-1]
		pm.cache.Remove(pageNum)
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
	pm.cache.Remove(pageNum)
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
	headerData := make([]byte, pm.pageSize)
	if err := pm.header.WriteTo(headerData[:100]); err != nil {
		return err
	}
	_, err := pm.file.WriteAt(headerData, 0)
	return err
}

func (pm *PageManager) CacheStats() (hits, misses int, hitRate float64) {
	hits, misses = pm.cache.Stats()
	hitRate = pm.cache.HitRate()
	return
}

func (pm *PageManager) CacheSize() int {
	return pm.cache.Size()
}
