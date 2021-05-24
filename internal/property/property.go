package property

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/version"
)

type Property struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (p Property) Validate() error {
	if len(p.Type) == 0 {
		return errors.New("type must be set")
	}
	if len(p.Value) == 0 {
		return errors.New("value must be set")
	}
	var raw json.RawMessage
	if err := json.Unmarshal(p.Value, &raw); err != nil {
		return fmt.Errorf("value is not valid json: %v", err)
	}
	return nil
}

type Package struct {
	PackageName string `json:"packageName"`
	Version     string `json:"version"`
}

type PackageRequired struct {
	PackageName  string `json:"packageName"`
	VersionRange string `json:"versionRange"`
}

type Channel struct {
	Name     string `json:"name"`
	Replaces string `json:"replaces,omitempty"`
}

type GVK struct {
	Group   string `json:"group"`
	Kind    string `json:"kind"`
	Version string `json:"version"`
}

type GVKRequired struct {
	Group   string `json:"group"`
	Kind    string `json:"kind"`
	Version string `json:"version"`
}

type Skips string
type SkipRange string

type BundleObject struct {
	File `json:",inline"`
}

type File struct {
	ref  string
	data []byte
}

type fileJSON struct {
	Ref  string `json:"ref,omitempty"`
	Data []byte `json:"data,omitempty"`
}

func (f *File) UnmarshalJSON(data []byte) error {
	var t fileJSON
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	if len(t.Ref) > 0 && len(t.Data) > 0 {
		return errors.New("fields 'ref' and 'data' are mutually exclusive")
	}
	f.ref = t.Ref
	f.data = t.Data
	return nil
}

func (f File) MarshalJSON() ([]byte, error) {
	return json.Marshal(fileJSON{
		Ref:  f.ref,
		Data: f.data,
	})
}

func (f File) IsRef() bool {
	return len(f.ref) > 0
}

func (f File) GetRef() string {
	return f.ref
}

func (f File) GetData(root, cwd string) ([]byte, error) {
	if !f.IsRef() {
		return f.data, nil
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if filepath.IsAbs(f.ref) {
		return nil, fmt.Errorf("reference must be a relative path")
	}
	refAbs, err := filepath.Abs(filepath.Join(cwd, f.ref))
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(refAbs, rootAbs) {
		return nil, fmt.Errorf("reference %q must be within root %q", refAbs, rootAbs)
	}
	return ioutil.ReadFile(refAbs)
}

type Properties struct {
	Packages         []Package
	PackagesRequired []PackageRequired
	Channels         []Channel
	GVKs             []GVK
	GVKsRequired     []GVKRequired
	Skips            []Skips
	SkipRanges       []SkipRange
	BundleObjects    []BundleObject

	Others []Property
}

const (
	TypePackage         = "olm.package"
	TypePackageRequired = "olm.package.required"
	TypeChannel         = "olm.channel"
	TypeGVK             = "olm.gvk"
	TypeGVKRequired     = "olm.gvk.required"
	TypeSkips           = "olm.skips"
	TypeSkipRange       = "olm.skipRange"
	TypeBundleObject    = "olm.bundle.object"
)

func Parse(in []Property) (*Properties, error) {
	var out Properties
	for i, prop := range in {
		switch prop.Type {
		case TypePackage:
			var p Package
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Packages = append(out.Packages, p)
		case TypePackageRequired:
			var p PackageRequired
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.PackagesRequired = append(out.PackagesRequired, p)
		case TypeChannel:
			var p Channel
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Channels = append(out.Channels, p)
		case TypeGVK:
			var p GVK
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.GVKs = append(out.GVKs, p)
		case TypeGVKRequired:
			var p GVKRequired
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.GVKsRequired = append(out.GVKsRequired, p)
		case TypeSkips:
			var p Skips
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Skips = append(out.Skips, p)
		case TypeSkipRange:
			var p SkipRange
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.SkipRanges = append(out.SkipRanges, p)
		case TypeBundleObject:
			var p BundleObject
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.BundleObjects = append(out.BundleObjects, p)
		default:
			var p json.RawMessage
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Others = append(out.Others, prop)
		}
	}
	return &out, nil
}

func Deduplicate(in []Property) []Property {
	type key struct {
		typ   string
		value string
	}

	props := map[key]Property{}
	var out []Property
	for _, p := range in {
		k := key{p.Type, string(p.Value)}
		if _, ok := props[k]; ok {
			continue
		}
		props[k] = p
		out = append(out, p)
	}
	return out
}

func Build(p interface{}) (*Property, error) {
	var (
		typ string
		val interface{}
	)
	if prop, ok := p.(*Property); ok {
		typ = prop.Type
		val = prop.Value
	} else {
		t := reflect.TypeOf(p)
		if t.Kind() != reflect.Ptr {
			return nil, errors.New("input must be a pointer to a type")
		}
		typ, ok = scheme[t]
		if !ok {
			return nil, fmt.Errorf("%s not a known property type registered with the scheme", t)
		}
		val = p
	}
	d, err := jsonMarshal(val)
	if err != nil {
		return nil, err
	}

	return &Property{
		Type:  typ,
		Value: d,
	}, nil
}

func MustBuild(p interface{}) Property {
	prop, err := Build(p)
	if err != nil {
		panic(err)
	}
	return *prop
}

func jsonMarshal(p interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	dec := json.NewEncoder(buf)
	dec.SetEscapeHTML(false)
	err := dec.Encode(p)
	if err != nil {
		return nil, err
	}
	out := &bytes.Buffer{}
	if err := json.Compact(out, buf.Bytes()); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func MustBuildPackage(name, version string) Property {
	return MustBuild(&Package{PackageName: name, Version: version})
}
func MustBuildPackageRequired(name, versionRange string) Property {
	return MustBuild(&PackageRequired{name, versionRange})
}
func MustBuildChannel(name, replaces string) Property {
	return MustBuild(&Channel{name, replaces})
}
func MustBuildGVK(group, version, kind string) Property {
	return MustBuild(&GVK{group, kind, version})
}
func MustBuildGVKRequired(group, version, kind string) Property {
	return MustBuild(&GVKRequired{group, kind, version})
}
func MustBuildSkips(skips string) Property {
	s := Skips(skips)
	return MustBuild(&s)
}
func MustBuildSkipRange(skipRange string) Property {
	s := SkipRange(skipRange)
	return MustBuild(&s)
}
func MustBuildBundleObjectRef(ref string) Property {
	return MustBuild(&BundleObject{File: File{ref: ref}})
}
func MustBuildBundleObjectData(data []byte) Property {
	return MustBuild(&BundleObject{File: File{data: data}})
}

type GVKs []GVK

var _ sort.Interface = GVKs{}

func (gvks GVKs) Len() int      { return len(gvks) }
func (gvks GVKs) Swap(i, j int) { gvks[i], gvks[j] = gvks[j], gvks[i] }
func (gvks GVKs) Less(i, j int) bool {
	if gvks[i].Group == gvks[j].Group {
		if gvks[i].Kind == gvks[j].Kind {
			return version.CompareKubeAwareVersionStrings(gvks[i].Version, gvks[j].Version) < 0
		}
		return gvks[i].Kind < gvks[j].Kind
	}
	return gvks[i].Group < gvks[j].Group
}
