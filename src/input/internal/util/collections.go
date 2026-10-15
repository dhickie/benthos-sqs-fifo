package util

// TransformDeref returns a collection of item pointers as a collection of their dereferenced values
func TransformDeref[Tin any](in []*Tin) []Tin {
	return Transform(in, func(v *Tin) Tin {
		return *v
	})
}

// Transform projects a collection of items into another form, determined by the provided function
func Transform[Tin any, Tout any](in []Tin, fn func(Tin) Tout) []Tout {
	out := make([]Tout, len(in))
	for i, v := range in {
		out[i] = fn(v)
	}

	return out
}
