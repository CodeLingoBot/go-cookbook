package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func bitfieldGet(data []byte, bits, offset uint64, isSigned bool) int64 {
	if offset > uint64(len(data)*8) {
		return 0
	}

	if isSigned {
		return getSignedBitfield(data, offset, bits)
	}

	// It's safe to store result in int64.
	return int64(getUnsignedBitfield(data, offset, bits))
}

func bitfieldSet(data []byte, newValue, bits, offset uint64, isSigned bool) (int64, []byte) {
	if offset > uint64(len(data)*8) {
		return 0, data
	}

	oldValue := bitfieldGet(data, bits, offset, isSigned)
	return oldValue, setUnsignedBitfield(data, offset, bits, newValue)
}

func bitfieldIncrby(data []byte, incr, bits, offset uint64, bitfieldOverflowType string, isSigned bool) (int64, []byte) {
	if offset > uint64(len(data)*8) {
		return 0, data
	}

	if isSigned {
		oldValue := getUnsignedBitfield(data, offset, bits)
		newValue, overflowFlag := checkSignedBitfieldOverflow(int64(oldValue), int64(incr), bits, bitfieldOverflowType)
		if overflowFlag != 0 && strings.ToLower(bitfieldOverflowType) == "fail" {
			return 0, nil
		}
		return int64(newValue), setUnsignedBitfield(data, offset, bits, uint64(newValue))
	}

	oldValue := getUnsignedBitfield(data, offset, bits)
	newValue, overflowFlag := checkUnsignedBitfieldOverflow(oldValue, int64(incr), bits, bitfieldOverflowType)
	if overflowFlag != 0 && strings.ToLower(bitfieldOverflowType) == "fail" {
		return 0, nil
	}
	return int64(newValue), setUnsignedBitfield(data, offset, bits, newValue)
}

// The type MUST format as: [u|i][1-64].
// The supported types are up to 64 bits for signed integers,
// and up to 63 bits for unsigned integers.
// Therefore, bitfield only support u63 and i64.
func checkAndGetBitfieldType(bitfieldType string) (uint64, error) {
	syntaxErr := fmt.Errorf("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
	if len(bitfieldType) != 2 && len(bitfieldType) != 3 {
		return 0, syntaxErr
	}

	if bitfieldType[0] != 'u' && bitfieldType[0] != 'i' {
		return 0, syntaxErr
	}

	bits, err := strconv.ParseUint(bitfieldType[1:], 10, 64)
	if err != nil {
		return 0, syntaxErr
	}

	switch bitfieldType[0] {
	case 'u':
		if bits > 63 {
			return 0, syntaxErr
		}
	case 'i':
		if bits > 64 {
			return 0, syntaxErr
		}
	}

	return bits, nil
}

// The offset format is like '100' or '#100'.
func checkAndGetBitfieldOffset(bitfieldOffset string) (uint64, error) {
	if bitfieldOffset[0] == '#' {
		bitfieldOffset = bitfieldOffset[1:]
	}

	offset, err := strconv.ParseUint(bitfieldOffset, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}

	return offset, nil
}

func checkAndGetBitfieldNewValue(newValue string) (uint64, error) {
	if newValue[0] == '-' {
		n, err := strconv.ParseInt(newValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		return uint64(n), nil
	}

	n, err := strconv.ParseUint(newValue, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	return n, nil
}

func checkBitfieldOverflowType(bitfieldOverflowType string) error {
	switch strings.ToLower(bitfieldOverflowType) {
	case "wrap", "sat", "fail":
		return nil
	default:
		return fmt.Errorf("ERR Invalid OVERFLOW type specified")
	}
}

func printBytes(input []byte) {
	for i := range input {
		fmt.Printf("0x%x ", input[i])
	}
	fmt.Println()
}

func printBytesInBinary(input []byte) {
	for _, b := range input {
		for i := 7; i >= 0; i-- {
			if (b & (1 << uint(i))) > 0 {
				fmt.Printf("1")
			} else {
				fmt.Printf("0")
			}
		}
		fmt.Printf("|")
	}
	fmt.Println()
}

func uint64ToBytes(input uint64) []byte {
	var result []byte
	var unit byte

	for i := 63; i >= 0; i-- {
		if input&(1<<uint(i)) > 0 {
			unit = unit | (1 << uint(i%8))
		}
		if (unit != 0) && (i%8 == 0) {
			result = append(result, unit)
			unit = 0
		}
	}
	return result
}

func setUnsignedBitfield(data []byte, offset, bits, value uint64) []byte {
	value &= 0xFFFFFFFFFFFFFFFF >> (64 - bits)

	// If the offset+bits beyond the original data length, expand the array of bytes with 0x00
	if offset+bits > uint64(len(data)*8) {
		var numOfExpandBytes int
		numOfExpandBits := offset + bits - uint64(len(data)*8)
		if numOfExpandBits%8 > 0 {
			numOfExpandBytes = int(numOfExpandBits/8 + 1)
		} else {
			numOfExpandBytes = int(numOfExpandBits / 8)
		}
		for i := 0; i < numOfExpandBytes; i++ {
			data = append(data, byte(0x00))
		}
	}

	for i := 0; i < int(bits); i++ {
		var bitval byte = 0
		if (value & (uint64)(1<<(bits-1-uint64(i)))) > 0 {
			bitval = 1
		}
		byteIndex := offset >> 3
		bit := 7 - (offset & 0x7)
		byteval := data[byteIndex]
		byteval &= ^(1 << bit)
		byteval |= bitval << bit
		data[byteIndex] = byteval
		offset++
	}
	return data
}

func getUnsignedBitfield(data []byte, offset, bits uint64) uint64 {
	var value uint64
	for i := 0; i < int(bits); i++ {
		byteIndex := offset >> 3
		bit := 7 - (offset & 0x7)
		if byteIndex > uint64(len(data)-1) {
			value <<= 1
		} else {
			byteval := data[byteIndex]
			bitval := (byteval >> bit) & 1
			value = (value << 1) | uint64(bitval)
		}
		offset++
	}
	return value
}

func getSignedBitfield(data []byte, offset, bits uint64) int64 {
	unsignedValue := getUnsignedBitfield(data, offset, bits)
	sign := unsignedValue & (1 << uint(bits-1))

	// Get the 2's complement of effective data
	if sign > 0 {
		return int64((^unsignedValue)&(0xFFFFFFFFFFFFFFFF>>(64-bits))+1) * -1
	}
	return int64(unsignedValue)
}

func checkUnsignedBitfieldOverflow(value uint64, incr int64, bits uint64, overflowType string) (uint64, int) {
	var max uint64
	if bits == 64 {
		max = math.MaxUint64
	} else {
		max = (uint64)(1<<bits) - 1
	}

	maxincr := max - value
	minincr := -int64(value)

	// Overflow process
	if value > max || (incr > 0 && incr > int64(maxincr)) {
		switch overflowType {
		case "wrap", "fail":
			return uint64(int64(value)+incr) & ^uint64(0xFFFFFFFFFFFFFFFF<<bits), 1
		case "sat":
			return max, 1
		}
	}

	// Underflow process
	if incr < 0 && incr < minincr {
		switch overflowType {
		case "wrap", "fail":
			return uint64(int64(value)+incr) & ^uint64(0xFFFFFFFFFFFFFFFF<<bits), 1
		case "sat":
			return 0, -1
		}
	}

	return uint64(incr) + value, 0
}

// Translate from redis-5.0.3 src/bitops.c
func checkSignedBitfieldOverflow(value int64, incr int64, bits uint64, overflowType string) (int64, int) {
	var (
		max, min, maxincr, minincr int64
	)

	if bits == 64 {
		max = math.MaxInt64
	} else {
		max = int64(1<<(bits-1)) - 1
	}
	min = -max - 1
	maxincr = max - value
	minincr = min - value

	// Overflow process
	if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
		switch overflowType {
		case "wrap", "fail":
			msb := uint64(1 << (bits - 1))
			mask := uint64(0xFFFFFFFFFFFFFFFF << (bits - 1))
			c := uint64(value) + uint64(incr)
			if c&msb > 0 {
				c |= mask
			} else {
				c &= ^mask
			}
			return int64(c), 1
		case "sat":
			return max, 1
		}
	}

	// Underflow process
	if value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
		switch overflowType {
		case "wrap", "fail":
			msb := uint64(1 << (bits - 1))
			mask := uint64(0xFFFFFFFFFFFFFFFF << (bits - 1))
			c := uint64(value) + uint64(incr)
			if c&msb > 0 {
				c |= mask
			} else {
				c &= ^mask
			}
			return int64(c), 1
		case "sat":
			return min, -1
		}
	}

	return incr + value, 0
}

func main() {
	var (
		result              []int64
		value               = []byte{0x38, 0x38, 0x33}
		currentOverflowType = "wrap"
		bitfieldOperations  []func()
		syntaxError         bool
	)

	input := "BITFIELD m set u8 #0 39 set u8 #1 39 set u8 #2 39 get u24 0"
	command := strings.Split(input, " ")

	for i := 2; i < len(command); {
		c2 := strings.ToLower(command[i])
		if c2 == "get" {
			if i+1 >= len(command) || i+2 >= len(command) {
				fmt.Println("ERR syntax error")
				syntaxError = true
				break
			}
			bitfieldType := command[i+1]
			bitfieldOffset := command[i+2]
			bits, err := checkAndGetBitfieldType(bitfieldType)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			offset, err := checkAndGetBitfieldOffset(bitfieldOffset)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			if bitfieldOffset[0] != '#' {
				offset, _ = strconv.ParseUint(bitfieldOffset[:], 10, 64)
			} else {
				offset, _ = strconv.ParseUint(bitfieldOffset[1:], 10, 64)
				offset = offset * bits
			}

			isSigned := false
			if bitfieldType[0] == 'i' {
				isSigned = true
			}

			bitfieldOperations = append(bitfieldOperations, func() {
				result = append(result, bitfieldGet(value, bits, offset, isSigned))
			})
			i = i + 3
			continue
		} else if c2 == "set" {
			if i+1 >= len(command) || i+2 >= len(command) || i+3 > len(command) {
				fmt.Println("ERR syntax error")
				syntaxError = true
				break
			}
			bitfieldType := command[i+1]
			bitfieldOffset := command[i+2]
			bitfieldNewValue := command[i+3]

			bits, err := checkAndGetBitfieldType(bitfieldType)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			offset, err := checkAndGetBitfieldOffset(bitfieldOffset)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			newValue, err := checkAndGetBitfieldNewValue(bitfieldNewValue)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			if bitfieldOffset[0] != '#' {
				offset, _ = strconv.ParseUint(bitfieldOffset[:], 10, 64)
			} else {
				offset, _ = strconv.ParseUint(bitfieldOffset[1:], 10, 64)
				offset = offset * bits
			}

			isSigned := false
			if bitfieldType[0] == 'i' {
				isSigned = true
			}

			bitfieldOperations = append(bitfieldOperations, func() {
				r, data := bitfieldSet(value, newValue, bits, offset, isSigned)
				result = append(result, r)
				value = data
			})
			i = i + 4
			continue
		} else if c2 == "incrby" {
			if i+1 >= len(command) || i+2 >= len(command) || i+3 > len(command) {
				fmt.Println("ERR syntax error")
				syntaxError = true
				break
			}

			bitfieldType := command[i+1]
			bitfieldOffset := command[i+2]
			bitfieldIncrement := command[i+3]

			bits, err := checkAndGetBitfieldType(bitfieldType)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			offset, err := checkAndGetBitfieldOffset(bitfieldOffset)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			incr, err := checkAndGetBitfieldNewValue(bitfieldIncrement)
			if err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}

			if bitfieldOffset[0] != '#' {
				offset, _ = strconv.ParseUint(bitfieldOffset[:], 10, 64)
			} else {
				offset, _ = strconv.ParseUint(bitfieldOffset[1:], 10, 64)
				offset = offset * bits
			}

			isSigned := false
			if bitfieldType[0] == 'i' {
				isSigned = true
			}

			bitfieldOperations = append(bitfieldOperations, func() {
				r, data := bitfieldIncrby(value, incr, bits, offset, currentOverflowType, isSigned)
				result = append(result, r)
				// Reset to the default type
				currentOverflowType = "wrap"
				if len(data) > 0 {
					value = data
				}
			})
			i = i + 4
			continue
		} else if c2 == "overflow" {
			if i+1 >= len(command) {
				fmt.Println("ERR syntax error")
				syntaxError = true
				break
			}
			overflowType := command[i+1]
			if err := checkBitfieldOverflowType(overflowType); err != nil {
				fmt.Println(err)
				syntaxError = true
				break
			}
			bitfieldOperations = append(bitfieldOperations, func() {
				currentOverflowType = overflowType
			})
			i = i + 2
			continue
		} else {
			fmt.Println("ERR syntax error")
			syntaxError = true
			break
		}
	}

	if syntaxError {
		os.Exit(0)
	}

	for _, op := range bitfieldOperations {
		op()
	}

	for i, r := range result {
		fmt.Printf("%d: %d\n", i, r)
	}
}
