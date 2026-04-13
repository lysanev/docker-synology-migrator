# Docker Synology Migrator

Windows GUI utility for migrating Docker containers and their data from Synology to Debian over SSH.

## What it does

- connects to Synology and Debian over SSH
- loads all source containers into a GUI list with checkbox selection
- shows related containers based on Compose project, shared networks, and Compose dependency labels
- highlights risky incomplete selections directly in the container list
- shows per-container details, migration status, and a persistent session log file
- supports Dry Run mode that validates access and builds a migration plan without copying data or changing hosts
- reads container configuration with `docker inspect`
- exports Docker images from Synology and loads them on Debian
- archives bind mounts and Docker volume data from Synology
- uploads archives to Debian
- restores bind mounts and named volumes on Debian
- generates `compose.yaml`
- starts containers with `docker compose up -d`

## Requirements

- SSH enabled on Synology
- SSH enabled on Debian
- Docker installed on both hosts
- `docker compose` plugin or `docker-compose` on Debian
- enough free disk space and RAM on the Windows machine running the utility for streamed archives in memory

## Important limitations

- migration is best-effort for standalone containers; swarm-specific settings are not restored
- external Docker networks referenced by containers must already exist on Debian
- source containers are not deleted automatically
- you can optionally stop source containers during backup for more consistent application data

## Build

```powershell
powershell -ExecutionPolicy Bypass -File D:\codex\build-migrator.ps1
```

## Run

Run [DockerSynologyMigrator.exe](D:\codex\dist\DockerSynologyMigrator.exe) without arguments.

## GUI flow

1. Enter Synology SSH credentials.
2. Enter Debian SSH credentials.
3. Click `Load Containers`.
4. Review the container list, related containers, and details panel.
5. Select containers to migrate. Use `Select Related` when containers work together.
6. Optional: enable `Dry Run` to validate access and build a plan only.
7. Click `Start Migration` or `Build Dry Run Plan`.

The application writes a session log into the `logs` folder next to the EXE and also shows it inside the GUI.
Row colors in the container list:

- green: selected and ready
- amber: selected but missing related containers
- sand: related to selected containers but not selected yet
