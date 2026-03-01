package main

import (
	"os"
	"path/filepath"
)

type HistoryManager struct {
	history  []string
	filename string
}

func NewHistoryManager() *HistoryManager {
	home, _ := os.UserHomeDir()
	return &HistoryManager{
		history:  []string{},
		filename: filepath.Join(home, ".sqlvibe_history"),
	}
}

func (h *HistoryManager) Add(cmd string) {
	h.history = append(h.history, cmd)
}

func (h *HistoryManager) GetHistory() []string {
	return h.history
}

func (h *HistoryManager) Load() error {
	data, err := os.ReadFile(h.filename)
	if err != nil {
		return err
	}
	lines := splitLines(string(data))
	h.history = append(h.history, lines...)
	return nil
}

func (h *HistoryManager) Save() error {
	file, err := os.Create(h.filename)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, cmd := range h.history {
		file.WriteString(cmd + "\n")
	}
	return nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			if i > start {
				lines = append(lines, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
