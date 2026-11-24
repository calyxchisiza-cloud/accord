package accord

type hold struct {
	owner string
	token uint64
}

type outcome struct {
	index uint64
	token uint64
	value string
	found bool
	err   error
}

type machine struct {
	kv    map[string]string
	locks map[string]hold
}

func newMachine() *machine {
	return &machine{
		kv:    map[string]string{},
		locks: map[string]hold{},
	}
}

func (m *machine) apply(index uint64, e Entry) outcome {
	out := outcome{index: index}
	switch e.Kind {
	case "", "nop":
		return out
	case "put":
		m.kv[e.Key] = e.Value
		out.value = e.Value
		out.found = true
		return out
	case "del":
		delete(m.kv, e.Key)
		return out
	case "read":
		v, ok := m.kv[e.Key]
		out.value = v
		out.found = ok
		return out
	case "lock":
		cur, held := m.locks[e.Key]
		if held && cur.owner != e.Owner {
			out.err = ErrHeld
			out.token = cur.token
			return out
		}
		m.locks[e.Key] = hold{owner: e.Owner, token: index}
		out.token = index
		return out
	case "unlock":
		cur, held := m.locks[e.Key]
		if !held || cur.owner != e.Owner {
			out.err = ErrNotHeld
			return out
		}
		delete(m.locks, e.Key)
		return out
	default:
		out.err = ErrInvalid
		return out
	}
}

func validName(s string) error {
	if len(s) < 1 || len(s) > 64 {
		return ErrInvalid
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9' || r == '-':
			if i == 0 {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
	}
	return nil
}

func validOwner(s string) error {
	if len(s) < 1 || len(s) > 128 {
		return ErrInvalid
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return ErrInvalid
		}
	}
	return nil
}

func validate(e Entry) error {
	switch e.Kind {
	case "put":
		if err := validName(e.Key); err != nil {
			return err
		}
		if len(e.Value) > maxValue {
			return ErrTooLarge
		}
	case "del", "read":
		return validName(e.Key)
	case "lock", "unlock":
		if err := validName(e.Key); err != nil {
			return err
		}
		return validOwner(e.Owner)
	default:
		return ErrInvalid
	}
	return nil
}
