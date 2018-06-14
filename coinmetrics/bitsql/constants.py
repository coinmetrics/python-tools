HASH_PRECISION = 78
CHAINWORK_PRECISION = 48
OUTPUT_VALUE_PRECISION = 32
MAX_ADDRESS_LENGTH = 64
OUTPUT_TYPES = {
	"nulldata": 0,
	"nonstandard": 1,
	"pubkeyhash": 2,
	"pubkey": 3,
	"multisig": 4,
	"scripthash": 5,
	"witness_v0_keyhash": 6,
	"witness_v0_scripthash": 7,
	# decred
	"stakegen": 64,
	"stakesubmission": 65,
	"sstxcommitment": 66,
	"sstxchange": 67,
	"stakerevoke": 68,
	"pubkeyalt": 69,
}

BLOCK_TIMES = {
	"btc": 600,
	"pivx": 60,
	"dash": 160,
	"ltc": 150,
	"vtc": 150,
	"xmr": 120,
	"etc": 15,
	"eth": 15,
	"zec": 150,
	"dgb": 15,
	"doge": 60,
	"btg": 600,
	"bch": 600,
	"xvg": 30,
	"dcr": 150,
}

SUPPORTED_ASSETS = ["btc", "ltc", "vtc", "dash", "doge", "zec", "dgb", "xvg", "pivx", "dcr", "bch", "btg"]