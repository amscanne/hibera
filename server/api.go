package server

func Run(addr string, port int) error {
	err := RunGossip(addr, port)
	if err != nil {
		return err
	}

	err = RunHTTP(addr, port)
	if err != nil {
		return err
	}

	return nil
}
