package basispoints

import (
	"encoding/json"
	"os"
	"testing"
)

// 发布源的插件标识必须匹配动态库，否则 CPA 无法定位对应平台的 ZIP。
func TestPluginStoreRegistryMatchesPlugin(t *testing.T) {
	raw, err := os.ReadFile("../../registry.json")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		SchemaVersion int `json:"schema_version"`
		Plugins       []struct {
			ID, Name, Description, Author, Repository, Version string
			Install                                            struct{ Type string }
		} `json:"plugins"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	if registry.SchemaVersion != 1 || len(registry.Plugins) != 1 {
		t.Fatalf("unexpected registry schema or plugin count: %+v", registry)
	}
	plugin := registry.Plugins[0]
	if plugin.ID != PluginID || plugin.Install.Type != "github-release" {
		t.Fatalf("registry does not match published plugin: %+v", plugin)
	}
	if plugin.Repository != "https://github.com/anlostsheep/cpa-plugin-oai-basispoints" {
		t.Fatalf("unexpected release repository: %q", plugin.Repository)
	}
	if plugin.Name == "" || plugin.Description == "" || plugin.Author == "" {
		t.Fatal("required registry display metadata is missing")
	}
	if plugin.Version != "" {
		t.Fatal("registry version must come from the latest GitHub release")
	}
}
