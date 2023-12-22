package rdb

import "errors"

var errCorruptContent = errors.New("corrupt compressed string content")

// decompressLZ77 decompresses the inp buffer, which has the expected
// out length.
// The algorithm used is the Level-1 compression of the https://github.com/ariya/FastLZ,
// which is a variant of the LZ77.
// The inp consists of one or more instructions. There are 3 types of
// instructions, which are described by their first bytes.
//   - 000xxxxx -> Literal run
//     The last 5 bits of the first byte + 1 describes the amount of bytes needs to be
//     copied from the inp to out.
//   - 111xxxxx -> Long match
//     There is a long match with back reference. We will be copying some bytes from
//     the out to out. The match length(the amount of data we need to copy) is equal
//     to 9 + second byte of the instruction. The back reference we will start
//     copying (from the tail of the out) is equal to the 256 * the last 5 bits of the
//     first byte of the instruction + the third byte of the instruction.
//   - xxxxxxxx -> Short match
//     There is a short match with back reference. We will be copying some bytes from
//     the out to out. The match length(the amount of data we need to copy) is equal
//     to 2 + the first 3 bits of the first byte of the instruction. The back reference
//     we will start copying (from the tail of the out) is equal to the
//     256 * the last 5 bits of the first byte of the instruction + the second byte of
//     the instruction.
//
// Note that, for long and short matches, the match length from the back reference index
// can be greater than the amount of data we have in the out. In that case, we need to
// think out as a dynamic buffer that grows as we append bytes to it.
func decompressLZ77(inp []byte, outLen int) ([]byte, error) {
	inpIdx := 0
	inpLen := len(inp)
	outIdx := 0
	out := make([]byte, 0)

	for inpIdx < inpLen {
		ctrl := inp[inpIdx]
		inpIdx++

		if ctrl < 32 {
			// Literal run, there are ctrl + 1 many bytes to copy from inp to out
			run := int(ctrl + 1)

			if inpLen < inpIdx+run {
				return nil, errCorruptContent
			}

			if outLen < outIdx+run {
				return nil, errCorruptContent
			}

			out = append(out, inp[inpIdx:inpIdx+run]...)
			inpIdx += run
			outIdx += run
		} else {
			// Back reference, we will be copying some bytes from the out to out
			matchLen := int(ctrl>>5) + 2

			if inpLen <= inpIdx {
				return nil, errCorruptContent
			}

			if matchLen == 9 {
				// Long match, match len is 9 + next byte
				matchLen += int(inp[inpIdx])
				inpIdx++

				if inpLen <= inpIdx {
					return nil, errCorruptContent
				}
			}

			backRef := outIdx - (int(ctrl&0x1F) << 8) - 1
			backRef -= int(inp[inpIdx])
			inpIdx++

			if outLen < outIdx+matchLen {
				return nil, errCorruptContent
			}

			if backRef < 0 {
				return nil, errCorruptContent
			}

			if backRef+matchLen < outIdx {
				// We have all the data we need to copy in the out buffer
				out = append(out, out[backRef:backRef+matchLen]...)
				outIdx += matchLen
			} else {
				// We need to copy more data than what we currently have in out
				outIdx += matchLen
				for matchLen > 0 {
					out = append(out, out[backRef])
					backRef++
					matchLen--
				}
			}
		}
	}

	if outIdx != outLen {
		return nil, errCorruptContent
	}

	return out, nil
}
