package main

import (
    "fmt"
    "strings"
)

func parseComputerSize(size string) (int, error) {
    oneKBytes := 1 << 10
    oneMBytes := 1 << 20
    oneGBytes := 1 << 30
    var (
        count int
        bits  byte
    )
    if _, err := fmt.Sscanf(strings.ToLower(size), "%d%c", &count, &bits); err != nil {
        return 0, err
    }
    switch bits {
    case 'k':
        return count * oneKBytes, nil
    case 'm':
        return count * oneMBytes, nil
    case 'g':
        return count * oneGBytes, nil
    }
    return 0, fmt.Errorf("[Config] Format error")
}
