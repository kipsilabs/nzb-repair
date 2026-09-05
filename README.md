# nzb-repair

<a href="https://www.buymeacoffee.com/qbt52hh7sjd"><img src="https://img.buymeacoffee.com/button-api/?text=Buy me a coffee&emoji=☕&slug=qbt52hh7sjd&button_colour=FFDD00&font_colour=000000&font_family=Comic&outline_colour=000000&coffee_colour=ffffff" /></a>

A tool to repair incomplete NZB files by reuploading the missing articles using the par2 repair.

## Usage

1. Create a config file `config.yaml` with your Usenet provider details:

```yaml
download_providers:
  - host: news.example.com
    port: 563
    username: your_username
    password: your_password
    connections: 10
    tls: true
upload_providers:
  - host: upload.example.com
    port: 563
    username: your_username
    password: your_password
    connections: 5
    tls: true
```

2. Run the tool:

**Single File Repair:**

```sh
nzb-repair -c config.yaml path/to/your.nzb
```

**Watch Mode (Monitor a directory):**

It will scan a directory in configurable interval for files to repair.

```sh
nzb-repair watch -c config.yaml -d /path/to/watch/directory
```

**Options:**

_Flags applicable to both modes:_

- `-c, --config`: Config file path (required)
- `-o, --output`: Output file path or directory for repaired nzb files (optional, defaults vary by mode: next to input file for single repair, `repaired/` subdirectory for watch mode)
- `--tmp-dir`: Temporary directory for processing files (optional, defaults to system temp dir)
- `-v, --verbose`: Enable verbose logging (optional)

_Flags specific to Watch Mode:_

- `-d, --dir`: Directory to watch for nzb files (required for watch mode)
- `-b, --db`: Path to the sqlite database file for the queue (optional, defaults to `queue.db`)

## Development Setup

To set up the project for development, follow these steps:

1. Clone the repository:

```sh
git clone https://github.com/kipsilabs/nzb-repair.git
cd nntpcli
```

2. Install dependencies:

```sh
go mod download
```

3. Run tests:

```sh
make test
```

4. Lint the code:

```sh
make lint
```

5. Generate mocks and other code:

```sh
make generate
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request. See the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
