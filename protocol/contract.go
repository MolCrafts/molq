// Package protocol loads the shared JSON Schema contract.
package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"

	_ "embed"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed contract.json
var raw []byte

// Error is one stable RPC failure.
type Error struct {
	Kind    string
	Message string
	Outcome string
	Context map[string]any
}

func (e *Error) Error() string { return e.Message }

// Fail builds an error whose outcome is not_applied.
func Fail(kind, message string, context map[string]any) *Error {
	return FailOutcome(kind, message, "not_applied", context)
}

// FailOutcome builds an error with an explicit mutation outcome.
func FailOutcome(kind, message, outcome string, context map[string]any) *Error {
	if context == nil {
		context = map[string]any{}
	}
	return &Error{Kind: kind, Message: message, Outcome: outcome, Context: context}
}

// Data is the ErrorData object carried in a JSON-RPC error.
func (e *Error) Data() map[string]any {
	context := e.Context
	if context == nil {
		context = map[string]any{}
	}
	outcome := e.Outcome
	if outcome == "" {
		outcome = "not_applied"
	}
	return map[string]any{
		"kind": e.Kind, "message": e.Message, "outcome": outcome, "context": context,
	}
}

// Contract is the embedded protocol document.
type Contract struct {
	raw     []byte
	release string
	methods []string
	schemas map[string]*jsonschema.Schema
}

var load = sync.OnceValue(func() *Contract {
	var doc struct {
		Release string `json:"release"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(err)
	}
	names, err := methodNames(raw)
	if err != nil {
		panic(err)
	}
	compiler := jsonschema.NewCompiler()
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		panic(err)
	}
	if err := compiler.AddResource("https://molcrafts.org/molq/protocol", parsed); err != nil {
		panic(err)
	}
	schemas := make(map[string]*jsonschema.Schema, len(names))
	for _, name := range names {
		compiled, err := compiler.Compile("https://molcrafts.org/molq/protocol#/methods/" + name)
		if err != nil {
			panic(err)
		}
		schemas[name] = compiled
	}
	return &Contract{
		raw:     bytes.Clone(raw),
		release: doc.Release,
		methods: names,
		schemas: schemas,
	}
})

// Current returns the process-wide contract.
func Current() *Contract { return load() }

// Raw is the canonical schema document.
func (c *Contract) Raw() []byte { return c.raw }

// Release is the packaged molq release.
func (c *Contract) Release() string { return c.release }

// Methods lists RPC methods in schema order.
func (c *Contract) Methods() []string { return c.methods }

// ValidateDocument checks one value against a schema pointer such as #/$defs/JobSpec.
func (c *Contract) ValidateDocument(value any, pointer string) error {
	if err := bounded(value, 0); err != nil {
		return err
	}
	compiled, err := c.pointer(pointer)
	if err != nil {
		return err
	}
	if err := compiled.Validate(value); err != nil {
		return Fail("INVALID_INPUT", "Request does not match the method schema", map[string]any{"reason": err.Error()})
	}
	return nil
}

func (c *Contract) pointer(pointer string) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(c.raw))
	if err != nil {
		return nil, err
	}
	if err := compiler.AddResource("https://molcrafts.org/molq/protocol", parsed); err != nil {
		return nil, err
	}
	compiled, err := compiler.Compile("https://molcrafts.org/molq/protocol" + pointer)
	if err != nil {
		return nil, Fail("INVALID_INPUT", "Request does not match the method schema", map[string]any{"reason": err.Error()})
	}
	return compiled, nil
}

// Validate checks nesting, NUL, and the method's parameter schema.
func (c *Contract) Validate(method string, params any) error {
	if err := bounded(params, 0); err != nil {
		return err
	}
	schema := c.schemas[method]
	if schema == nil {
		return Fail("METHOD_NOT_FOUND", "Unknown RPC method", map[string]any{"method": method})
	}
	if err := schema.Validate(params); err != nil {
		return Fail("INVALID_INPUT", "Request does not match the method schema", map[string]any{
			"method": method,
			"reason": err.Error(),
		})
	}
	return nil
}

func bounded(value any, depth int) error {
	if depth > 64 {
		return Fail("INVALID_INPUT", "Schema nesting exceeds the protocol bound", nil)
	}
	switch item := value.(type) {
	case string:
		if strings.Contains(item, "\x00") {
			return Fail("INVALID_INPUT", "NUL is not legal in target commands, environment or paths", nil)
		}
	case map[string]any:
		for key, child := range item {
			if err := bounded(key, depth+1); err != nil {
				return err
			}
			if err := bounded(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range item {
			if err := bounded(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func methodNames(document []byte) ([]string, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(document, &top); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(top["methods"]))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, Fail("INVALID_INPUT", "Protocol methods must be an object", nil)
	}
	var names []string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		names = append(names, token.(string))
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			return nil, err
		}
	}
	return names, nil
}
