package util

const (
	m = uint32(2147483647)
)

type Random struct {
	seed uint32
}

func (r *Random) Next() uint32 {
	const a = 16807
	product := r.seed * a
	r.seed = (product >> 31) + (product & m)
	if r.seed > m {
		r.seed -= m
	}
	return r.seed
}

func (r *Random) Uniform(n int) uint32 {
	return r.Next() % uint32(n)
}

func (r *Random) OneIn(n int) bool {
	return r.Next()%uint32(n) == 0
}

func (r *Random) Skewed(maxLog int) uint32 {
	return r.Uniform(1 << r.Uniform(maxLog+1))
}

func NewRandom(s uint32) *Random {
	r := &Random{seed: s & 0x7fffffff}
	if r.seed == 0 || r.seed == m {
		r.seed = 1
	}
	return r
}
