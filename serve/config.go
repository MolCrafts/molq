package serve

import "github.com/MolCrafts/molq/protocol"

func (s *Service) configure(method string, params map[string]any) (any, error) {
	if s.config == nil {
		return nil, protocol.Fail("INVALID_INPUT", "Configuration file is required", nil)
	}
	switch method {
	case "config.reload":
		if err := s.config.Reload(); err != nil {
			return nil, err
		}
		s.applySettings()
		s.Notify()
		return map[string]any{"reloaded": true}, nil
	case "presets.list":
		return s.config.List("presets")
	case "config.get":
		return s.config.Get("settings", stringField(params["key"]))
	case "presets.get":
		return s.config.Get("presets", stringField(params["key"]))
	case "config.set":
		result, err := s.config.Set("settings", stringField(params["key"]), params["value"])
		if err != nil {
			return nil, err
		}
		s.applySettings()
		s.Notify()
		return result, nil
	case "presets.set":
		result, err := s.config.Set("presets", stringField(params["key"]), params["value"])
		if err != nil {
			return nil, err
		}
		s.Notify()
		return result, nil
	default:
		return nil, protocol.Fail("METHOD_NOT_FOUND", "Unknown RPC method", map[string]any{"method": method})
	}
}

func (s *Service) applySettings() {
	s.ensure()
	if s.config == nil {
		return
	}
	s.obs.SetMaxEntries(s.config.Entries(4096))
	s.obs.Clear()
}
