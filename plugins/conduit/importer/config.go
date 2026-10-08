package importer

// Config is unmarshalled from conduit.yml importer.config.
type Config struct {
	ArchivePath  string `yaml:"archive_path"`
	GenesisFile  string `yaml:"genesis_file"`
	Mode         string `yaml:"mode"` // offline | follow
	PollInterval string `yaml:"poll_interval"` // Go duration, e.g. 200ms
	WaitTimeout  string `yaml:"wait_timeout"`  // Go duration; empty/0s = no timeout
}
