package rdb

const Version uint16 = 12

type Type uint8

const (
	TypeString              Type = 0
	TypeList                Type = 1
	TypeSet                 Type = 2
	TypeZset                Type = 3
	TypeHash                Type = 4
	TypeZset2               Type = 5
	TypeModule2             Type = 7 // module pre ga with type 6 is not supported
	TypeHashZipmap          Type = 9 // type 8 seems unused by Redis
	TypeListZiplist         Type = 10
	TypeSetIntset           Type = 11
	TypeZsetZiplist         Type = 12
	TypeHashZiplist         Type = 13
	TypeListQuicklist       Type = 14
	TypeStreamListpacks     Type = 15
	TypeHashListpack        Type = 16
	TypeZsetListpack        Type = 17
	TypeListQuicklist2      Type = 18
	TypeStreamListpacks2    Type = 19
	TypeSetListpack         Type = 20
	TypeStreamListpacks3    Type = 21
	TypeHashMetadataPreGa   Type = 22 // pre-ga type is not supported
	TypeHashListpackExPreGa Type = 23 // pre-ga type is not supported.
	TypeHashMetadata        Type = 24
	TypeHashListpackEx      Type = 25
)

const (
	typeOpCodeFunction2     Type = 245
	typeOpCodeFunctionPreGA Type = 246
	typeOpCodeModuleAux     Type = 247
	typeOpCodeIdle          Type = 248
	typeOpCodeFreq          Type = 249
	typeOpCodeAux           Type = 250
	typeOpCodeResizeDB      Type = 251
	typeOpCodeExpireTimeMS  Type = 252
	typeOpCodeExpireTime    Type = 253
	typeOpCodeSelectDB      Type = 254
	typeOpCodeEOF           Type = 255
)

const (
	len6Bit         uint8 = 0b00000000
	len14Bit        uint8 = 0b01000000
	len32Or64Bit    uint8 = 0b10000000
	lenEncodedValue uint8 = 0b11000000
)

const (
	len32Bit uint8 = 0b10000000
	len64Bit uint8 = 0b10000001
)
const (
	len6BitMax  uint64 = 1<<6 - 1
	len14BitMax uint64 = 1<<14 - 1
	len32BitMax uint64 = 1<<32 - 1
)

const (
	lenEncodingInt8  uint64 = 0
	lenEncodingInt16 uint64 = 1
	lenEncodingInt32 uint64 = 2
	lenEncodingLZF   uint64 = 3
)

const (
	zipmapLenBig uint8 = 254
	zipmapLenEnd uint8 = 255
	zipmapEnd    uint8 = 255
)

const (
	ziplistEnd        uint8  = 255
	ziplistLenBig     uint16 = 65535
	ziplistPrevLenBig uint8  = 254
)

const (
	ziplistEnc6BitStrLen  uint8 = 0b00000000
	ziplistEnc14BitStrLen uint8 = 0b01000000
	ziplistEnc32BitStrLen uint8 = 0b10000000
)

const (
	ziplistEncInt8  uint8 = 0b11111110
	ziplistEncInt16 uint8 = 0b11000000
	ziplistEncInt24 uint8 = 0b11110000
	ziplistEncInt32 uint8 = 0b11010000
	ziplistEncInt64 uint8 = 0b11100000
)

const (
	intsetEncInt16 uint32 = 2
	intsetEncInt32 uint32 = 4
	intsetEncInt64 uint32 = 8
)

const (
	listpackEnd    uint8  = 255
	listpackLenBig uint16 = 65535
)

const (
	listpackEncUint7 uint8 = 0b00000000
	listpackEncInt13 uint8 = 0b11000000

	listpackEncInt16 uint8 = 0b11110001
	listpackEncInt24 uint8 = 0b11110010
	listpackEncInt32 uint8 = 0b11110011
	listpackEncInt64 uint8 = 0b11110100

	listpackEnc6bitStrLen  uint8 = 0b10000000
	listpackEnc12bitStrLen uint8 = 0b11100000
	listpackEnc32bitStrLen uint8 = 0b11110000
)

const (
	quicklist2NodePlain  uint64 = 1
	quicklist2NodePacked uint64 = 2
)

const (
	moduleOpCodeEOF    uint64 = 0
	moduleOpCodeSInt   uint64 = 1
	moduleOpCodeUInt   uint64 = 2
	moduleOpCodeFloat  uint64 = 3
	moduleOpCodeDouble uint64 = 4
	moduleOpCodeString uint64 = 5
)

const jsonModuleID uint64 = 5035677737576115200

const (
	jsonModuleV0 uint64 = 0
	jsonModuleV3 uint64 = 3
)

const (
	jsonModuleV0NodeNull    uint64 = 1
	jsonModuleV0NodeString  uint64 = 2
	jsonModuleV0NodeNumber  uint64 = 4
	jsonModuleV0NodeInteger uint64 = 8
	jsonModuleV0NodeBoolean uint64 = 16
	jsonModuleV0NodeDict    uint64 = 32
	jsonModuleV0NodeArray   uint64 = 64
	jsonModuleV0NodeKeyVal  uint64 = 128
)

type ModuleMarker string

const (
	EmptyModuleMarker ModuleMarker = ""
	JSONModuleMarker  ModuleMarker = "json"
)

const (
	streamItemFlagDeleted    int = 1
	streamItemFlagSameFields int = 2
)
