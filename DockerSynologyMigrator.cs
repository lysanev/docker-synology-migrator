using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Web.Script.Serialization;
using Renci.SshNet;

internal static class MigratorCore
{
    internal const string DefaultTargetRoot = "/opt/docker-migration";
    internal const string DefaultVirtualMachineTargetRoot = "/opt/vm-migration";
    private const string ManagedComposeDirectoryName = "compose.d";
    private const string LegacyComposeFileName = "000-legacy-compose.yaml";
    private const string ManagedComposeFileLabel = "com.docker-synology-migrator.compose_file";
    private const string ManagedByLabel = "com.docker-synology-migrator.managed";
    private static readonly JavaScriptSerializer Json = new JavaScriptSerializer { MaxJsonLength = int.MaxValue };
    private static readonly Dictionary<string, string> EmbeddedAssemblies = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        { "Renci.SshNet", "DockerMigrator.Resources.Renci.SshNet.dll" },
        { "BouncyCastle.Cryptography", "DockerMigrator.Resources.BouncyCastle.Cryptography.dll" },
        { "Microsoft.Bcl.AsyncInterfaces", "DockerMigrator.Resources.Microsoft.Bcl.AsyncInterfaces.dll" },
        { "System.Formats.Asn1", "DockerMigrator.Resources.System.Formats.Asn1.dll" },
        { "System.Threading.Tasks.Extensions", "DockerMigrator.Resources.System.Threading.Tasks.Extensions.dll" },
        { "System.Buffers", "DockerMigrator.Resources.System.Buffers.dll" },
        { "System.Memory", "DockerMigrator.Resources.System.Memory.dll" },
        { "System.ValueTuple", "DockerMigrator.Resources.System.ValueTuple.dll" },
        { "System.Runtime.CompilerServices.Unsafe", "DockerMigrator.Resources.System.Runtime.CompilerServices.Unsafe.dll" },
        { "System.Numerics.Vectors", "DockerMigrator.Resources.System.Numerics.Vectors.dll" }
    };
    internal static Action<string> LogHandler { get; set; }

    internal static void Initialize()
    {
        AppDomain.CurrentDomain.AssemblyResolve -= ResolveEmbeddedAssembly;
        AppDomain.CurrentDomain.AssemblyResolve += ResolveEmbeddedAssembly;
    }

    private static Assembly ResolveEmbeddedAssembly(object sender, ResolveEventArgs args)
    {
        var requested = new AssemblyName(args.Name).Name;
        string resourceName;
        if (!EmbeddedAssemblies.TryGetValue(requested, out resourceName))
        {
            return null;
        }

        var assembly = Assembly.GetExecutingAssembly();
        using (var stream = assembly.GetManifestResourceStream(resourceName))
        {
            if (stream == null)
            {
                return null;
            }

            var bytes = new byte[stream.Length];
            stream.Read(bytes, 0, bytes.Length);
            return Assembly.Load(bytes);
        }
    }

    internal static List<ContainerDefinition> DiscoverContainers(ConnectionInfoData source, string targetRoot)
    {
        using (var sourceClient = CreateClient(source))
        {
            Log("Connecting to Synology...");
            sourceClient.Connect();
            EnsureDockerAvailable(sourceClient, source, "Synology");

            var containerNames = GetContainerNames(sourceClient, source, null, true);
            var definitions = new List<ContainerDefinition>();
            foreach (var containerName in containerNames)
            {
                Log("Inspecting {0}...", containerName);
                var inspectJson = ExecuteCommand(sourceClient, "docker inspect " + ShellQuote(containerName), source);
                definitions.Add(ParseContainerDefinition(inspectJson, containerName, targetRoot));
            }

            PopulateContainerSizeEstimates(sourceClient, source, definitions);
            return definitions.OrderBy(d => d.Name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    internal static List<ContainerDefinition> DiscoverTargetContainers(ConnectionInfoData target, string targetRoot)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Debian for containers...");
            targetClient.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");

            var containerNames = GetContainerNames(targetClient, target, null, true);
            var definitions = new List<ContainerDefinition>();
            foreach (var containerName in containerNames)
            {
                Log("Inspecting target container {0}...", containerName);
                var inspectJson = ExecuteCommand(targetClient, "docker inspect " + ShellQuote(containerName), target);
                definitions.Add(ParseContainerDefinition(inspectJson, containerName, targetRoot));
            }

            return definitions.OrderBy(d => d.Name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    internal static void StartTargetContainer(ConnectionInfoData target, string containerName)
    {
        ExecuteTargetContainerCommand(target, containerName, "start", "Starting");
    }

    internal static void StopTargetContainer(ConnectionInfoData target, string containerName)
    {
        ExecuteTargetContainerCommand(target, containerName, "stop", "Stopping");
    }

    internal static void DeleteTargetContainer(ConnectionInfoData target, string containerName)
    {
        ExecuteTargetContainerCommand(target, containerName, "rm -f", "Deleting");
    }

    internal static List<VirtualMachineDefinition> DiscoverVirtualMachines(ConnectionInfoData source)
    {
        using (var sourceClient = CreateClient(source))
        {
            Log("Connecting to Synology for virtual machines...");
            sourceClient.Connect();
            EnsureSynologyVirtualMachineToolsAvailable(sourceClient, source);

            var virshNames = CollectSynologyVirtualMachineNames(sourceClient, source);
            var apiDefinitions = DiscoverVirtualMachinesFromSynologyApi(sourceClient, source);
            var filesystemDefinitions = DiscoverVirtualMachinesFromFilesystem(sourceClient, source);
            var names = new HashSet<string>(virshNames, StringComparer.OrdinalIgnoreCase);

            foreach (var definition in filesystemDefinitions)
            {
                if (!string.IsNullOrWhiteSpace(definition.Name))
                {
                    names.Add(definition.Name);
                }

                if (!string.IsNullOrWhiteSpace(definition.Uuid))
                {
                    names.Add(definition.Uuid);
                }
            }

            if (names.Count == 0)
            {
                Log("No virtual machines were reported by Synology VMM over virsh.");
            }

            var result = new List<VirtualMachineDefinition>();
            foreach (var name in names.OrderBy(item => item, StringComparer.OrdinalIgnoreCase))
            {
                VirtualMachineDefinition definition;
                if (TryInspectVirtualMachineDefinition(sourceClient, source, name, out definition))
                {
                    result.Add(definition);
                }
            }

            MergeVirtualMachineDefinitions(result, filesystemDefinitions);
            MergeVirtualMachineDefinitions(result, apiDefinitions);
            DeduplicateVirtualMachineDefinitionsByAlias(result);
            EnrichVirtualMachineDefinitions(sourceClient, source, result);
            DeduplicateVirtualMachineDefinitionsByAlias(result);
            Log("Filesystem VM definitions discovered on Synology: {0}", filesystemDefinitions.Count);
            Log("Synology API VM definitions discovered: {0}", apiDefinitions.Count);
            Log("Synology VM inventory summary: virsh={0}, api-definitions={1}, candidates={2}, merged={3}",
                virshNames.Count, apiDefinitions.Count, names.Count, result.Count);

            return result
                .OrderBy(item => NormalizeOptionalValue(item.DisplayName) ?? item.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
    }

    internal static List<ProxmoxVirtualMachineDefinition> DiscoverProxmoxVirtualMachines(ConnectionInfoData target)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Proxmox for virtual machines...");
            targetClient.Connect();
            EnsureProxmoxVirtualMachineToolsAvailable(targetClient, target);

            var output = ExecuteCommand(targetClient, "qm list", target);
            var list = ParseProxmoxVirtualMachineList(output);
            foreach (var vm in list)
            {
                var configText = ExecuteCommand(targetClient, "qm config " + vm.VmId.ToString(CultureInfo.InvariantCulture), target);
                ApplyProxmoxVirtualMachineConfig(vm, configText);
            }

            return list
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                .ThenBy(item => item.VmId)
                .ToList();
        }
    }

    internal static List<string> DiscoverProxmoxStorages(ConnectionInfoData target)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Proxmox for storages...");
            targetClient.Connect();
            EnsureProxmoxVirtualMachineToolsAvailable(targetClient, target);

            var output = ExecuteCommand(targetClient, "pvesm status --content images", target);
            return ParseProxmoxStorageNames(output);
        }
    }

    internal static List<string> DiscoverProxmoxBridges(ConnectionInfoData target)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Proxmox for bridges...");
            targetClient.Connect();
            EnsureProxmoxVirtualMachineToolsAvailable(targetClient, target);

            var output = ExecuteCommand(targetClient, "ip -o link show | awk -F': ' '{print $2}'", target);
            return output
                .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(item => item.Split('@')[0].Trim())
                .Where(item => item.StartsWith("vmbr", StringComparison.OrdinalIgnoreCase) ||
                               item.StartsWith("br", StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
    }

    internal static void StartTargetVirtualMachine(ConnectionInfoData target, int vmId)
    {
        ExecuteTargetVirtualMachineCommand(target, vmId, "start", "Starting");
    }

    internal static void StopTargetVirtualMachine(ConnectionInfoData target, int vmId)
    {
        ExecuteTargetVirtualMachineCommand(target, vmId, "stop", "Stopping");
    }

    internal static void DeleteTargetVirtualMachine(ConnectionInfoData target, int vmId)
    {
        ExecuteTargetVirtualMachineCommand(target, vmId, "destroy " + vmId.ToString(CultureInfo.InvariantCulture) + " --destroy-unreferenced-disks 1 --purge 1", "Deleting", true);
    }

    internal static void RunVirtualMachineMigration(VirtualMachineMigrationOptions options)
    {
        using (var sourceClient = CreateClient(options.Source))
        using (var targetClient = CreateClient(options.Target))
        {
            Log("Connecting to Synology...");
            sourceClient.Connect();
            Log("Connecting to Proxmox...");
            targetClient.Connect();
            EnsureSynologyVirtualMachineToolsAvailable(sourceClient, options.Source);
            EnsureProxmoxVirtualMachineToolsAvailable(targetClient, options.Target);

            var plan = BuildVirtualMachineMigrationPlan(sourceClient, targetClient, options);
            LogVirtualMachinePlan(plan, options);

            if (options.DryRun)
            {
                Log("Dry run completed. No virtual machines were copied and no changes were applied.");
                return;
            }

            using (var targetSftp = CreateSftp(options.Target))
            {
                targetSftp.Connect();
                var stagingRoot = CreateStagingRoot(targetClient, options.Target);
                Log("VM target staging path: {0}", stagingRoot);
                PrepareTargetRoot(targetClient, options.TargetRoot, options.Target);

                foreach (var vm in plan.VirtualMachines)
                {
                    var shouldRestartSourceVm = false;
                    if (options.StopVirtualMachinesDuringExport && vm.Running)
                    {
                        Log("Stopping source VM {0}...", vm.Name);
                        ExecuteSynologyVirshCommand(sourceClient, options.Source, "shutdown " + ShellQuote(vm.Name));
                        WaitForVirtualMachinePowerState(sourceClient, options.Source, vm.Name, false, 300);
                        shouldRestartSourceVm = true;
                    }

                    try
                    {
                        var vmId = GetNextProxmoxVmId(targetClient, options.Target);
                        vm.AssignedTargetVmId = vmId;
                        Log("Creating Proxmox VM {0} ({1})...", vm.Name, vmId);
                        CreateTargetVirtualMachineSkeleton(targetClient, options.Target, vm, vmId, options.TargetBridge);
                        ImportVirtualMachineDisks(sourceClient, options.Source, targetClient, options.Target, targetSftp, options.TargetRoot, stagingRoot, vm, options.TargetStorage);

                        if (options.StartImportedVirtualMachines)
                        {
                            Log("Starting imported Proxmox VM {0} ({1})...", vm.Name, vmId);
                            ExecuteCommand(targetClient, "qm start " + vmId.ToString(CultureInfo.InvariantCulture), options.Target);
                        }
                    }
                    finally
                    {
                        if (shouldRestartSourceVm)
                        {
                            Log("Starting source VM {0}...", vm.Name);
                            ExecuteSynologyVirshCommand(sourceClient, options.Source, "start " + ShellQuote(vm.Name));
                        }
                    }
                }
            }
        }
    }

    internal static void UpdateTargetContainerNetwork(ConnectionInfoData target, string targetRoot, string containerName, ContainerNetworkOverride networkOverride)
    {
        var normalizedContainerName = NormalizeOptionalValue(containerName);
        if (normalizedContainerName == null)
        {
            throw new InvalidOperationException("Target container name is required.");
        }

        using (var targetClient = CreateClient(target))
        using (var targetSftp = CreateSftp(target))
        {
            Log("Connecting to Debian for target network update...");
            targetClient.Connect();
            targetSftp.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");
            EnsureComposeAvailable(targetClient, target, "Debian");

            PrepareTargetRoot(targetClient, targetRoot, target);
            MigrateLegacyComposeIfNeeded(targetClient, targetRoot, target);

            var definition = InspectContainerDefinition(targetClient, target, normalizedContainerName, targetRoot);
            var composeFileName = ResolveManagedComposeFileName(definition, targetClient, target, targetRoot);
            EnsureManagedComposeLabels(definition, composeFileName);
            ApplyNetworkOverride(definition, new List<ContainerNetworkOverride> { networkOverride });

            var composeFile = BuildComposeFileArtifact(definition);
            UploadComposeFiles(targetClient, targetSftp, target, targetRoot, CreateStagingRoot(targetClient, target), new List<ComposeFileArtifact> { composeFile });

            var composeCommand = BuildComposeUpCommand(targetRoot, definition.Name, true);
            Log("Applying network update for target container {0}...", definition.Name);
            ExecuteCommand(targetClient, composeCommand, target);

            if (!definition.Running)
            {
                Log("Restoring stopped state for target container {0}...", definition.Name);
                ExecuteCommand(targetClient, "docker stop " + ShellQuote(definition.Name), target);
            }
        }
    }

    internal static List<string> DiscoverDockerNetworks(ConnectionInfoData target)
    {
        return DiscoverDockerNetworkDefinitions(target)
            .Where(item => item != null && IsUserDefinedNetwork(item.Name))
            .Select(item => item.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    internal static List<DockerNetworkDefinition> DiscoverDockerNetworkDefinitions(ConnectionInfoData target)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Debian for Docker networks...");
            targetClient.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");

            var output = ExecuteCommand(targetClient, "docker network ls --format '{{.Name}}'", target);
            var names = output
                .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(item => item.Trim())
                .Where(item => item.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var result = new List<DockerNetworkDefinition>();
            foreach (var name in names)
            {
                Log("Inspecting network {0}...", name);
                var inspectJson = ExecuteCommand(targetClient, "docker network inspect " + ShellQuote(name), target);
                result.Add(ParseDockerNetworkDefinition(inspectJson));
            }

            return result.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    internal static string PreviewDockerNetworkCreateCommand(DockerNetworkCreateRequest request)
    {
        return BuildDockerNetworkCreateCommand(request);
    }

    internal static void CreateDockerNetwork(ConnectionInfoData target, DockerNetworkCreateRequest request)
    {
        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Debian for network create...");
            targetClient.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");
            Log("Creating {0} network {1}...", request.Driver, request.Name);
            ExecuteCommand(targetClient, BuildDockerNetworkCreateCommand(request), target);
        }
    }

    internal static void DeleteDockerNetwork(ConnectionInfoData target, string networkName)
    {
        var normalized = NormalizeOptionalValue(networkName);
        if (normalized == null)
        {
            throw new InvalidOperationException("Docker network name is required.");
        }

        if (!IsUserDefinedNetwork(normalized))
        {
            throw new InvalidOperationException("Built-in Docker networks host/bridge/none cannot be deleted from the app.");
        }

        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Debian for network delete...");
            targetClient.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");
            Log("Deleting network {0}...", normalized);
            ExecuteCommand(targetClient, "docker network rm " + ShellQuote(normalized), target);
        }
    }

    internal static void RunMigration(MigrationOptions options)
    {
        using (var sourceClient = CreateClient(options.Source))
        using (var targetClient = CreateClient(options.Target))
        {
            Log("Connecting to Synology...");
            sourceClient.Connect();
            Log("Connecting to Debian...");
            targetClient.Connect();
            var plan = BuildMigrationPlan(sourceClient, targetClient, options);
            LogPlan(plan, options);

            if (options.DryRun)
            {
                Log("Dry run completed. No data was copied and no changes were applied.");
                return;
            }

            using (var targetSftp = CreateSftp(options.Target))
            {
                targetSftp.Connect();
                var stagingRoot = CreateStagingRoot(targetClient, options.Target);
                Log("Target staging path: {0}", stagingRoot);
                PrepareTargetRoot(targetClient, options.TargetRoot, options.Target);
                MigrateLegacyComposeIfNeeded(targetClient, options.TargetRoot, options.Target);

                if (options.StopContainersDuringBackup)
                {
                    foreach (var definition in plan.Containers)
                    {
                        if (definition.Running)
                        {
                            Log("Stopping source container {0}...", definition.Name);
                            ExecuteCommand(sourceClient, "docker stop " + ShellQuote(definition.Name), options.Source);
                        }
                    }
                }

                try
                {
                    BackupImages(sourceClient, options.Source, targetSftp, plan.Images, stagingRoot);
                    foreach (var definition in plan.Containers)
                    {
                        BackupContainerData(sourceClient, options.Source, targetClient, options.Target, targetSftp, definition, stagingRoot);
                    }
                }
                finally
                {
                    if (options.StopContainersDuringBackup)
                    {
                        foreach (var definition in plan.Containers.Where(d => d.Running))
                        {
                            Log("Starting source container {0}...", definition.Name);
                            ExecuteCommand(sourceClient, "docker start " + ShellQuote(definition.Name), options.Source);
                        }
                    }
                }

                UploadComposeFiles(targetClient, targetSftp, options.Target, options.TargetRoot, stagingRoot, plan.ComposeFiles);

                foreach (var definition in plan.Containers)
                {
                    RestoreContainerData(targetClient, options.Target, definition, stagingRoot);
                    Log("Container data restored for {0}", definition.Name);
                }

                RestoreImages(targetClient, options.Target, plan.Images, stagingRoot);

                var composeCommand = BuildComposeUpCommand(options.TargetRoot, null, false);
                Log("Starting containers on Debian...");
                ExecuteCommand(targetClient, composeCommand, options.Target);
            }
        }
    }

    private static MigrationPlan BuildMigrationPlan(SshClient sourceClient, SshClient targetClient, MigrationOptions options)
    {
        EnsureDockerAvailable(sourceClient, options.Source, "Synology");
        EnsureDockerAvailable(targetClient, options.Target, "Debian");
        EnsureComposeAvailable(targetClient, options.Target, "Debian");

        var containerNames = GetContainerNames(sourceClient, options.Source, options.ContainerNames);
        if (containerNames.Count == 0)
        {
            throw new InvalidOperationException("No containers found for migration.");
        }

        Log("Containers selected: {0}", string.Join(", ", containerNames));

        var definitions = new List<ContainerDefinition>();
        foreach (var containerName in containerNames)
        {
            Log("Inspecting {0}...", containerName);
            var inspectJson = ExecuteCommand(sourceClient, "docker inspect " + ShellQuote(containerName), options.Source);
            var definition = ParseContainerDefinition(inspectJson, containerName, options.TargetRoot);
            ApplyNetworkOverride(definition, options.NetworkOverrides);
            definitions.Add(definition);
        }

        PopulateContainerSizeEstimates(sourceClient, options.Source, definitions);

        var imageNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var images = definitions
            .Select(d => d.Image)
            .Where(i => !string.IsNullOrWhiteSpace(i))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(i => new ImageArchive { Image = i, ArchiveFileName = BuildUniqueName("image-" + i, imageNames) + ".tar.gz" })
            .ToList();

        var composeFiles = definitions
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .Select(BuildComposeFileArtifact)
            .ToList();

        return new MigrationPlan
        {
            Containers = definitions,
            Images = images,
            ComposeFiles = composeFiles
        };
    }

    private static ComposeFileArtifact BuildComposeFileArtifact(ContainerDefinition definition)
    {
        var composeFileName = BuildManagedComposeFileName(definition.Name);
        EnsureManagedComposeLabels(definition, composeFileName);
        return new ComposeFileArtifact
        {
            ContainerName = definition.Name,
            FileName = composeFileName,
            RelativePath = ManagedComposeDirectoryName + "/" + composeFileName,
            ComposeYaml = BuildComposeYaml(new List<ContainerDefinition> { definition })
        };
    }

    private static void EnsureManagedComposeLabels(ContainerDefinition definition, string composeFileName)
    {
        if (definition == null)
        {
            return;
        }

        if (definition.Labels == null)
        {
            definition.Labels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        definition.Labels[ManagedByLabel] = "true";
        definition.Labels[ManagedComposeFileLabel] = composeFileName;
    }

    private static string ResolveManagedComposeFileName(ContainerDefinition definition, SshClient targetClient, ConnectionInfoData target, string targetRoot)
    {
        string composeFileName;
        if (definition != null &&
            definition.Labels != null &&
            definition.Labels.TryGetValue(ManagedComposeFileLabel, out composeFileName) &&
            !string.IsNullOrWhiteSpace(composeFileName))
        {
            return composeFileName.Trim();
        }

        var fallback = BuildManagedComposeFileName(definition == null ? null : definition.Name);
        var candidatePath = BuildManagedComposeDirectoryPath(targetRoot) + "/" + fallback;
        string output;
        string error;
        if (TryExecuteCommand(targetClient, "[ -f " + ShellQuote(candidatePath) + " ]", target, out output, out error))
        {
            return fallback;
        }

        throw new InvalidOperationException("This target container is not yet managed by the current per-container compose layout. Re-migrate the container with the current app version first.");
    }

    private static ContainerDefinition InspectContainerDefinition(SshClient client, ConnectionInfoData connectionInfo, string containerName, string targetRoot)
    {
        var inspectJson = ExecuteCommand(client, "docker inspect " + ShellQuote(containerName), connectionInfo);
        return ParseContainerDefinition(inspectJson, containerName, targetRoot);
    }

    private static void LogPlan(MigrationPlan plan, MigrationOptions options)
    {
        Log("Mode: {0}", options.DryRun ? "dry run (plan only)" : "live migration");
        Log("Target root: {0}", options.TargetRoot);
        Log("Compose file directory: {0}/{1}", options.TargetRoot.TrimEnd('/'), ManagedComposeDirectoryName);
        Log("Compose files in plan: {0}", string.Join(", ", plan.ComposeFiles.Select(item => item.RelativePath)));
        Log("Containers in plan: {0}", plan.Containers.Count);
        Log("Images in plan: {0}", plan.Images.Count);
        Log("Estimated migration size: {0}", FormatBytes(CalculateEstimatedMigrationBytes(plan.Containers)));

        if (options.StopContainersDuringBackup)
        {
            Log(options.DryRun
                ? "Source containers that would be temporarily stopped during backup: {0}"
                : "Source containers set for temporary stop during backup: {0}",
                plan.Containers.Count(item => item.Running));
        }
        else
        {
            Log("Source containers will not be stopped automatically.");
        }

        foreach (var container in plan.Containers.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase))
        {
            var networkText = container.Networks.Count == 0 ? "none" : string.Join(", ", container.Networks);
            var networkModeText = string.IsNullOrWhiteSpace(container.NetworkMode) ? "compose-networks" : container.NetworkMode;
            Log("Plan container {0}: image={1}; state={2}; mounts={3}; ports={4}; networks={5}; network_mode={6}; size={7}",
                container.Name,
                container.Image,
                string.IsNullOrWhiteSpace(container.Status) ? "unknown" : container.Status,
                container.Mounts.Count,
                container.PortBindings.Count,
                networkText,
                networkModeText,
                FormatBytes(container.EstimatedMigrationBytes));

            foreach (var mount in container.Mounts)
            {
                if (string.Equals(mount.Type, "bind", StringComparison.OrdinalIgnoreCase))
                {
                    Log("  bind {0} -> {1} => {2}", mount.SourcePath, mount.DestinationPath, mount.TargetSource);
                }
                else
                {
                    Log("  volume {0} -> {1}", mount.VolumeName, mount.DestinationPath);
                }
            }
        }

        if (plan.Images.Count > 0)
        {
            Log("Images to export and load: {0}", string.Join(", ", plan.Images.Select(item => item.Image)));
        }
        else
        {
            Log("Images to export and load: none");
        }
    }

    private static bool ReadYesNo(string prompt, bool defaultValue)
    {
        Console.Write(prompt + (defaultValue ? " [Y/n]: " : " [y/N]: "));
        var raw = (Console.ReadLine() ?? string.Empty).Trim().ToLowerInvariant();
        if (raw.Length == 0)
        {
            return defaultValue;
        }

        return raw == "y" || raw == "yes" || raw == "д" || raw == "да";
    }

    private static SecureString ReadPassword()
    {
        var password = new SecureString();
        while (true)
        {
            var key = Console.ReadKey(true);
            if (key.Key == ConsoleKey.Enter)
            {
                break;
            }

            if (key.Key == ConsoleKey.Backspace)
            {
                if (password.Length > 0)
                {
                    password.RemoveAt(password.Length - 1);
                    Console.Write("\b \b");
                }

                continue;
            }

            if (!char.IsControl(key.KeyChar))
            {
                password.AppendChar(key.KeyChar);
                Console.Write("*");
            }
        }

        password.MakeReadOnly();
        return password;
    }

    private static SshClient CreateClient(ConnectionInfoData data)
    {
        var client = new SshClient(data.Host, data.Port, data.Username, ToUnsecureString(data.Password));
        client.HostKeyReceived += (sender, args) => { args.CanTrust = true; };
        client.KeepAliveInterval = TimeSpan.FromSeconds(30);
        client.ConnectionInfo.Timeout = TimeSpan.FromSeconds(20);
        return client;
    }

    private static SftpClient CreateSftp(ConnectionInfoData data)
    {
        var client = new SftpClient(data.Host, data.Port, data.Username, ToUnsecureString(data.Password));
        client.HostKeyReceived += (sender, args) => { args.CanTrust = true; };
        client.ConnectionInfo.Timeout = TimeSpan.FromSeconds(20);
        return client;
    }

    private static List<string> GetContainerNames(SshClient sourceClient, ConnectionInfoData sourceInfo, List<string> requested, bool includeStopped = false)
    {
        if (requested != null && requested.Count > 0)
        {
            return requested;
        }

        var command = includeStopped ? "docker ps -a --format '{{.Names}}'" : "docker ps --format '{{.Names}}'";
        string output;
        string error;
        if (!TryExecuteCommand(sourceClient, command, sourceInfo, out output, out error))
        {
            Log("docker --format is unavailable on the remote host, using plain docker ps parsing.");
            var fallbackCommand = includeStopped ? "docker ps -a --no-trunc" : "docker ps --no-trunc";
            output = ExecuteCommand(sourceClient, fallbackCommand, sourceInfo);
            return ParseDockerPsNames(output);
        }

        return output
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
    }

    private static void ExecuteTargetContainerCommand(ConnectionInfoData target, string containerName, string dockerCommand, string logVerb)
    {
        var normalizedName = NormalizeOptionalValue(containerName);
        if (normalizedName == null)
        {
            throw new InvalidOperationException("Target container name is required.");
        }

        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Debian for container management...");
            targetClient.Connect();
            EnsureDockerAvailable(targetClient, target, "Debian");
            Log("{0} target container {1}...", logVerb, normalizedName);
            ExecuteCommand(targetClient, "docker " + dockerCommand + " " + ShellQuote(normalizedName), target);
        }
    }

    private static void ExecuteTargetVirtualMachineCommand(ConnectionInfoData target, int vmId, string qmCommand, string logVerb, bool commandIncludesVmId = false)
    {
        if (vmId <= 0)
        {
            throw new InvalidOperationException("Target VM ID is invalid.");
        }

        using (var targetClient = CreateClient(target))
        {
            Log("Connecting to Proxmox for VM management...");
            targetClient.Connect();
            EnsureProxmoxVirtualMachineToolsAvailable(targetClient, target);
            Log("{0} target VM {1}...", logVerb, vmId);
            var commandText = commandIncludesVmId
                ? "qm " + qmCommand
                : "qm " + qmCommand + " " + vmId.ToString(CultureInfo.InvariantCulture);
            ExecuteCommand(targetClient, commandText, target);
        }
    }

    private static void EnsureSynologyVirtualMachineToolsAvailable(SshClient client, ConnectionInfoData connectionInfo)
    {
        const string toolCheck =
            "VIRSH=$(command -v virsh 2>/dev/null || true); " +
            "QEMU_IMG=$(command -v qemu-img 2>/dev/null || true); " +
            "if [ -n \"$VIRSH\" ] && [ -n \"$QEMU_IMG\" ]; then " +
            "  if \"$VIRSH\" -c qemu:///system list --all --name >/dev/null 2>&1 || \"$VIRSH\" list --all --name >/dev/null 2>&1; then exit 0; fi; " +
            "fi; " +
            "echo \"virsh=${VIRSH:-missing}; qemu-img=${QEMU_IMG:-missing}\"; " +
            "exit 1";
        string output;
        string error;
        if (TryExecuteCommand(client, toolCheck, connectionInfo, out output, out error))
        {
            return;
        }

        throw new InvalidOperationException("Synology: Virtual Machine Manager tools are unavailable over SSH. " + BuildRemoteFailureDetails(output, error));
    }

    private static void EnsureProxmoxVirtualMachineToolsAvailable(SshClient client, ConnectionInfoData connectionInfo)
    {
        string output;
        string error;
        if (TryExecuteCommand(client, "qm list >/dev/null 2>&1 && pvesm status >/dev/null 2>&1", connectionInfo, out output, out error))
        {
            return;
        }

        throw new InvalidOperationException("Proxmox tools are unavailable over SSH. " + BuildRemoteFailureDetails(output, error));
    }

    private static bool TryInspectVirtualMachineDefinition(SshClient sourceClient, ConnectionInfoData source, string vmName, out VirtualMachineDefinition definition)
    {
        definition = null;
        try
        {
            Log("Inspecting VM {0}...", vmName);
            definition = InspectVirtualMachineDefinition(sourceClient, source, vmName);
            return true;
        }
        catch (Exception ex)
        {
            Log("[!] Inspecting VM {0} via dominfo failed: {1}: {2}", vmName, ex.GetType().Name, ex.Message);
        }

        try
        {
            Log("Inspecting VM {0} via inactive definition...", vmName);
            if (TryInspectVirtualMachineDefinitionInactive(sourceClient, source, vmName, out definition))
            {
                return true;
            }
        }
        catch (Exception ex)
        {
            Log("[!] Inspecting VM {0} via inactive definition failed: {1}: {2}", vmName, ex.GetType().Name, ex.Message);
        }

        return false;
    }

    private static bool TryInspectVirtualMachineDefinitionInactive(
        SshClient sourceClient,
        ConnectionInfoData source,
        string vmName,
        out VirtualMachineDefinition definition)
    {
        definition = null;

        string xml;
        if (!(TryExecuteSynologyVirshCommand(sourceClient, source, "dumpxml --inactive " + ShellQuote(vmName), out xml) ||
              TryExecuteSynologyVirshCommand(sourceClient, source, "dumpxml " + ShellQuote(vmName), out xml)))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(xml) || xml.IndexOf("<domain", StringComparison.OrdinalIgnoreCase) < 0)
        {
            return false;
        }

        definition = ParseVirtualMachineDefinitionFromXml(xml, vmName);
        definition.State = "shut off";
        definition.Running = false;

        string domInfoText;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "dominfo " + ShellQuote(vmName), out domInfoText))
        {
            var domInfo = ParseKeyValueOutput(domInfoText);
            definition.Uuid = NormalizeOptionalValue(GetDictionaryValue(domInfo, "UUID")) ?? definition.Uuid;
            definition.State = GetDictionaryValue(domInfo, "State") ?? definition.State;
            definition.Running = string.Equals(GetDictionaryValue(domInfo, "State"), "running", StringComparison.OrdinalIgnoreCase);
            definition.Autostart = string.Equals(GetDictionaryValue(domInfo, "Autostart"), "enable", StringComparison.OrdinalIgnoreCase) ||
                                   string.Equals(GetDictionaryValue(domInfo, "Autostart"), "yes", StringComparison.OrdinalIgnoreCase);
        }

        string domBlkDetailsInactiveOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist --details --inactive " + ShellQuote(vmName), out domBlkDetailsInactiveOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisks(domBlkDetailsInactiveOutput));
        }

        string domBlkSimpleInactiveOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist --inactive " + ShellQuote(vmName), out domBlkSimpleInactiveOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisksSimple(domBlkSimpleInactiveOutput));
        }

        foreach (var disk in definition.Disks)
        {
            PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, vmName, disk);
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            var runtimeConfigDisks = DiscoverVirtualMachineDisksFromRuntimeConfigsForVm(
                sourceClient,
                source,
                vmName,
                definition.Name,
                definition.Uuid,
                definition.DisplayName);
            MergeVirtualMachineDisks(definition.Disks, runtimeConfigDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
                PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, vmName, disk);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            var filesystemDisks = DiscoverVirtualMachineDisksByFilesystem(sourceClient, source, definition.Uuid, definition.Name, definition.DisplayName);
            MergeVirtualMachineDisks(definition.Disks, filesystemDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            }
        }

        if (definition.Interfaces == null || definition.Interfaces.Count == 0)
        {
            definition.Interfaces = ParseVirtualMachineInterfacesFromXml(xml);
        }

        return definition != null && !string.IsNullOrWhiteSpace(definition.Name);
    }

    private static VirtualMachineDefinition InspectVirtualMachineDefinition(SshClient sourceClient, ConnectionInfoData source, string vmName)
    {
        var domInfo = ParseKeyValueOutput(ExecuteSynologyVirshCommand(sourceClient, source, "dominfo " + ShellQuote(vmName)));
        var xml = ExecuteSynologyVirshCommand(sourceClient, source, "dumpxml " + ShellQuote(vmName));
        var definition = ParseVirtualMachineDefinitionFromXml(xml, vmName);
        definition.Uuid = NormalizeOptionalValue(GetDictionaryValue(domInfo, "UUID")) ?? definition.Uuid;
        definition.State = GetDictionaryValue(domInfo, "State") ?? definition.State ?? "unknown";
        definition.Running = string.Equals(GetDictionaryValue(domInfo, "State"), "running", StringComparison.OrdinalIgnoreCase);
        definition.Autostart = string.Equals(GetDictionaryValue(domInfo, "Autostart"), "enable", StringComparison.OrdinalIgnoreCase) ||
                               string.Equals(GetDictionaryValue(domInfo, "Autostart"), "yes", StringComparison.OrdinalIgnoreCase);

        var vcpusFromDomInfo = ParseIntValue(GetDictionaryValue(domInfo, "CPU(s)"));
        if (vcpusFromDomInfo > 0)
        {
            definition.Vcpus = vcpusFromDomInfo;
        }

        var memoryFromDomInfo = ParseMemoryKiB(GetDictionaryValue(domInfo, "Max memory"));
        if (memoryFromDomInfo > 0)
        {
            definition.MemoryKiB = memoryFromDomInfo;
        }

        definition.Disks = ParseVirtualMachineDisksFromXml(xml);

        string domBlkDetailsOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist --details " + ShellQuote(vmName), out domBlkDetailsOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisks(domBlkDetailsOutput));
        }

        string domBlkDetailsInactiveOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist --details --inactive " + ShellQuote(vmName), out domBlkDetailsInactiveOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisks(domBlkDetailsInactiveOutput));
        }

        string domBlkSimpleOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist " + ShellQuote(vmName), out domBlkSimpleOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisksSimple(domBlkSimpleOutput));
        }

        string domBlkSimpleInactiveOutput;
        if (TryExecuteSynologyVirshCommand(sourceClient, source, "domblklist --inactive " + ShellQuote(vmName), out domBlkSimpleInactiveOutput))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisksSimple(domBlkSimpleInactiveOutput));
        }

        foreach (var disk in definition.Disks)
        {
            PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, vmName, disk);
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            MergeVirtualMachineDisks(definition.Disks, ParseVirtualMachineDisksFromXmlCommandLine(xml));
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            var runtimeConfigDisks = DiscoverVirtualMachineDisksFromRuntimeConfigsForVm(
                sourceClient,
                source,
                vmName,
                definition.Name,
                definition.Uuid,
                definition.DisplayName);
            MergeVirtualMachineDisks(definition.Disks, runtimeConfigDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
                PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, vmName, disk);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks) && definition.Running)
        {
            var monitorDisks = DiscoverVirtualMachineDisksFromQemuMonitor(sourceClient, source, vmName);
            MergeVirtualMachineDisks(definition.Disks, monitorDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
                PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, vmName, disk);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks) && definition.Running)
        {
            var processIdText = GetDictionaryValue(domInfo, "PID") ??
                                GetDictionaryValue(domInfo, "Process ID") ??
                                GetDictionaryValue(domInfo, "ProcessID");
            if (string.IsNullOrWhiteSpace(processIdText))
            {
                processIdText = ResolveVirtualMachineProcessId(sourceClient, source, definition.Uuid, definition.Name);
            }

            var processDisks = DiscoverVirtualMachineDisksFromProcess(sourceClient, source, processIdText);
            MergeVirtualMachineDisks(definition.Disks, processDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            var filesystemDisks = DiscoverVirtualMachineDisksByFilesystem(sourceClient, source, definition.Uuid, definition.Name, definition.DisplayName);
            MergeVirtualMachineDisks(definition.Disks, filesystemDisks);
            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, vmName);
            }
        }

        if (!HasUsableVirtualMachineDisks(definition.Disks))
        {
            var diskTagCount = Regex.Matches(xml ?? string.Empty, "<disk\\b", RegexOptions.IgnoreCase).Count;
            var summaries = ExtractDiskBlockSummaries(xml).Take(3).ToList();
            Log("[!] VM {0}: disk detection returned zero disks. xml-disk-tags={1}", vmName, diskTagCount);
            if (summaries.Count > 0)
            {
                Log("[!] VM {0}: disk block summary => {1}", vmName, string.Join(" || ", summaries));
            }
        }

        definition.Interfaces = ParseVirtualMachineInterfaces(ExecuteSynologyVirshCommand(sourceClient, source, "domiflist " + ShellQuote(vmName)));
        if (definition.Interfaces.Count == 0)
        {
            definition.Interfaces = ParseVirtualMachineInterfacesFromXml(xml);
        }
        return definition;
    }

    private static VirtualMachineDefinition ParseVirtualMachineDefinitionFromXml(string xml, string fallbackName)
    {
        return new VirtualMachineDefinition
        {
            Name = GetRegexGroupValue(xml, "<name>\\s*([^<]+?)\\s*</name>", 1) ?? fallbackName,
            DisplayName = ResolveVirtualMachineDisplayName(xml, fallbackName),
            Uuid = GetRegexGroupValue(xml, "<uuid>\\s*([^<]+?)\\s*</uuid>", 1),
            State = "shut off",
            Running = false,
            Autostart = false,
            Vcpus = ParseIntValue(GetRegexGroupValue(xml, "<vcpu(?:\\s+[^>]*)?>\\s*([^<]+?)\\s*</vcpu>", 1)),
            MemoryKiB = ParseMemoryKiBFromDomainXml(xml),
            OsType = GetRegexGroupValue(xml, "<type(?:\\s+[^>]*)?>([^<]+)</type>", 1),
            MachineType = GetRegexGroupValue(xml, "machine=['\"]([^'\"]+)['\"]", 1),
            UsesUefi = Regex.IsMatch(xml ?? string.Empty, "<loader\\b", RegexOptions.IgnoreCase),
            Disks = ParseVirtualMachineDisksFromXml(xml),
            Interfaces = ParseVirtualMachineInterfacesFromXml(xml)
        };
    }

    private static List<VirtualMachineDiskDefinition> ParseVirtualMachineDisks(string output)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        foreach (var line in SplitDataLines(output))
        {
            var match = Regex.Match(line, "^\\s*(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.+?)\\s*$");
            if (!match.Success)
            {
                continue;
            }

            var deviceType = match.Groups[2].Value.Trim();
            var sourcePath = NormalizeOptionalValue(match.Groups[4].Value);
            if (string.Equals(deviceType, "cdrom", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(deviceType, "floppy", StringComparison.OrdinalIgnoreCase) ||
                IsVirtualMachineAuxiliaryDiskPath(sourcePath) ||
                sourcePath == null ||
                sourcePath == "-")
            {
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = deviceType,
                TargetName = match.Groups[3].Value.Trim(),
                SourcePath = sourcePath
            });
        }

        return result
            .OrderBy(item => item.TargetName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<VirtualMachineDiskDefinition> ParseVirtualMachineDisksSimple(string output)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        foreach (var line in (output ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0 ||
                trimmed.StartsWith("Target", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("---", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var match = Regex.Match(trimmed, "^(\\S+)\\s+(.+)$");
            if (!match.Success)
            {
                continue;
            }

            var sourcePath = NormalizeOptionalValue(match.Groups[2].Value);
            if (string.IsNullOrWhiteSpace(sourcePath) ||
                sourcePath == "-" ||
                IsVirtualMachineAuxiliaryDiskPath(sourcePath))
            {
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = match.Groups[1].Value.Trim(),
                SourcePath = sourcePath
            });
        }

        return result
            .GroupBy(item => item.TargetName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(item => item.TargetName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<string> ExtractDiskBlockSummaries(string xml)
    {
        var result = new List<string>();
        foreach (Match match in Regex.Matches(xml ?? string.Empty, "<disk\\b[\\s\\S]*?</disk>", RegexOptions.IgnoreCase))
        {
            var block = match.Value;
            var deviceType = GetRegexGroupValue(block, "<disk[^>]+device=['\"]([^'\"]+)['\"]", 1) ?? "unknown";
            var target = GetRegexGroupValue(block, "<target[^>]+dev=['\"]([^'\"]+)['\"]", 1) ?? "-";
            var source = GetRegexGroupValue(block, "<source[^>]+file=['\"]([^'\"]+)['\"]", 1) ??
                         GetRegexGroupValue(block, "<source[^>]+dev=['\"]([^'\"]+)['\"]", 1) ??
                         GetRegexGroupValue(block, "<source[^>]+path=['\"]([^'\"]+)['\"]", 1) ??
                         GetRegexGroupValue(block, "<source[^>]+name=['\"]([^'\"]+)['\"]", 1) ??
                         GetRegexGroupValue(block, "<source[^>]+volume=['\"]([^'\"]+)['\"]", 1) ??
                         "-";
            var pool = GetRegexGroupValue(block, "<source[^>]+pool=['\"]([^'\"]+)['\"]", 1);
            if (!string.IsNullOrWhiteSpace(pool))
            {
                source = pool + "/" + source;
            }

            result.Add("device=" + deviceType + ", target=" + target + ", source=" + source);
        }

        return result;
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksFromProcess(
        SshClient sourceClient,
        ConnectionInfoData source,
        string pidText)
    {
        var pid = ParseIntValue(pidText);
        if (pid <= 0)
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        string output;
        string error;
        var commandText = "if [ -r /proc/" + pid.ToString(CultureInfo.InvariantCulture) + "/cmdline ]; then tr '\\0' '\\n' < /proc/" + pid.ToString(CultureInfo.InvariantCulture) + "/cmdline; fi";
        if (!(TryExecuteCommand(sourceClient, commandText, source, out output, out error) ||
              TryExecuteCommandForcedSudo(sourceClient, commandText, source, out output, out error)))
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var configPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var deviceHints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tokens = (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(item => NormalizeOptionalValue(item))
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .ToList();

        for (var index = 0; index < tokens.Count; index++)
        {
            var line = tokens[index];
            if (line == null)
            {
                continue;
            }

            ExtractBlockDeviceHintsFromProcessToken(deviceHints, line);
            ExtractDiskPathCandidatesFromProcessToken(paths, line);

            if (string.Equals(line, "-readconfig", StringComparison.OrdinalIgnoreCase) &&
                index + 1 < tokens.Count)
            {
                AddProcessConfigPathCandidate(configPaths, tokens[index + 1]);
            }

            var inlineReadConfigMatch = Regex.Match(line, "^-readconfig(?:=|\\s+)(.+)$", RegexOptions.IgnoreCase);
            if (inlineReadConfigMatch.Success)
            {
                AddProcessConfigPathCandidate(configPaths, inlineReadConfigMatch.Groups[1].Value);
            }

            foreach (Match match in Regex.Matches(line, "(/run/libvirt/qemu/[^\\s'\",;]+\\.(?:xml|cfg|conf))", RegexOptions.IgnoreCase))
            {
                AddProcessConfigPathCandidate(configPaths, match.Groups[1].Value);
            }
        }

        var result = new List<VirtualMachineDiskDefinition>();
        foreach (var configPath in configPaths)
        {
            MergeVirtualMachineDisks(result, DiscoverVirtualMachineDisksFromRuntimeConfigFile(sourceClient, source, configPath));
        }

        string fdOutput;
        string fdError;
        var fdCommandText =
            "if [ -d /proc/" + pid.ToString(CultureInfo.InvariantCulture) + "/fd ]; then " +
            "for fd in /proc/" + pid.ToString(CultureInfo.InvariantCulture) + "/fd/*; do readlink \"$fd\" 2>/dev/null; done; " +
            "fi";
        if (TryExecuteCommand(sourceClient, fdCommandText, source, out fdOutput, out fdError) ||
            TryExecuteCommandForcedSudo(sourceClient, fdCommandText, source, out fdOutput, out fdError))
        {
            foreach (var raw in (fdOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                AddProcessDiskPathCandidate(paths, raw);
            }
        }

        string lsofOutput;
        string lsofError;
        var lsofCommandText =
            "if command -v lsof >/dev/null 2>&1; then lsof -p " + pid.ToString(CultureInfo.InvariantCulture) + " -Fn 2>/dev/null; fi";
        if (TryExecuteCommand(sourceClient, lsofCommandText, source, out lsofOutput, out lsofError) ||
            TryExecuteCommandForcedSudo(sourceClient, lsofCommandText, source, out lsofOutput, out lsofError))
        {
            foreach (var raw in (lsofOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var line = NormalizeOptionalValue(raw);
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                if (line.StartsWith("n", StringComparison.Ordinal))
                {
                    AddProcessDiskPathCandidate(paths, line.Substring(1));
                    continue;
                }

                ExtractDiskPathCandidatesFromProcessToken(paths, line);
            }
        }

        foreach (var candidatePath in DiscoverBlockDevicePathsByHints(sourceClient, source, deviceHints))
        {
            AddProcessDiskPathCandidate(paths, candidatePath);
        }

        MergeVirtualMachineDisks(result, paths
            .Select((path, index) => new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "vd" + (char)('a' + index),
                SourcePath = path
            })
            .ToList());

        if (result.Count == 0)
        {
            var sample = string.Join(" | ", tokens.Take(120));
            if (!string.IsNullOrWhiteSpace(sample))
            {
                Log("VM process fallback pid={0}: no usable disk paths from cmdline; sample args: {1}", pid, sample);
            }
        }

        return result;
    }

    private static void ExtractBlockDeviceHintsFromProcessToken(HashSet<string> target, string token)
    {
        var line = NormalizeOptionalValue(token);
        if (string.IsNullOrWhiteSpace(line))
        {
            return;
        }

        foreach (Match match in Regex.Matches(line, "wwpn=([^,\\s]+)", RegexOptions.IgnoreCase))
        {
            var value = NormalizeOptionalValue(match.Groups[1].Value);
            if (!string.IsNullOrWhiteSpace(value))
            {
                target.Add(value);
                target.Add(Regex.Replace(value, "[^0-9a-zA-Z]", string.Empty));
            }
        }

        foreach (Match match in Regex.Matches(line, "id=vdisk_([^,\\s]+)", RegexOptions.IgnoreCase))
        {
            var value = NormalizeOptionalValue(match.Groups[1].Value);
            if (!string.IsNullOrWhiteSpace(value))
            {
                target.Add(value);
                target.Add(Regex.Replace(value, "[^0-9a-zA-Z]", string.Empty));
            }
        }
    }

    private static List<string> DiscoverBlockDevicePathsByHints(
        SshClient sourceClient,
        ConnectionInfoData source,
        HashSet<string> hints)
    {
        var normalizedHints = (hints ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase))
            .Select(item => NormalizeOptionalValue(item))
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Select(item => Regex.Replace(item.ToLowerInvariant(), "[^0-9a-z]", string.Empty))
            .Where(item => item.Length >= 8)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (normalizedHints.Count == 0)
        {
            return new List<string>();
        }

        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var lsblkSamples = new List<string>();

        string lsblkOutput;
        string lsblkError;
        const string lsblkCommand = "if command -v lsblk >/dev/null 2>&1; then lsblk -P -o PATH,WWN,SERIAL,MODEL,KNAME,NAME 2>/dev/null; fi";
        if (TryExecuteCommand(sourceClient, lsblkCommand, source, out lsblkOutput, out lsblkError) ||
            TryExecuteCommandForcedSudo(sourceClient, lsblkCommand, source, out lsblkOutput, out lsblkError))
        {
            foreach (var line in (lsblkOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (Match match in Regex.Matches(line, "(\\w+)=\"([^\"]*)\""))
                {
                    values[match.Groups[1].Value] = NormalizeOptionalValue(match.Groups[2].Value);
                }

                var path = NormalizeOptionalValue(GetDictionaryValue(values, "PATH"));
                if (string.IsNullOrWhiteSpace(path) || !path.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var metadata = string.Join("|", new[]
                {
                    NormalizeOptionalValue(GetDictionaryValue(values, "WWN")),
                    NormalizeOptionalValue(GetDictionaryValue(values, "SERIAL")),
                    NormalizeOptionalValue(GetDictionaryValue(values, "MODEL")),
                    NormalizeOptionalValue(GetDictionaryValue(values, "KNAME")),
                    NormalizeOptionalValue(GetDictionaryValue(values, "NAME")),
                    path
                }.Where(item => !string.IsNullOrWhiteSpace(item)))
                    .ToLowerInvariant();
                var compactMetadata = Regex.Replace(metadata, "[^0-9a-z]", string.Empty);
                if (lsblkSamples.Count < 10)
                {
                    lsblkSamples.Add(
                        (NormalizeOptionalValue(GetDictionaryValue(values, "PATH")) ?? "-") + "|" +
                        (NormalizeOptionalValue(GetDictionaryValue(values, "WWN")) ?? "-") + "|" +
                        (NormalizeOptionalValue(GetDictionaryValue(values, "SERIAL")) ?? "-") + "|" +
                        (NormalizeOptionalValue(GetDictionaryValue(values, "MODEL")) ?? "-"));
                }

                if (normalizedHints.Any(hint => compactMetadata.Contains(hint)))
                {
                    result.Add(path);
                }
            }
        }

        string byIdOutput;
        string byIdError;
        const string byIdCommand =
            "for item in /dev/disk/by-id/* /dev/disk/by-path/*; do " +
            "if [ -e \"$item\" ]; then printf '%s -> %s\\n' \"$item\" \"$(readlink -f \"$item\" 2>/dev/null)\"; fi; " +
            "done";
        if (TryExecuteCommand(sourceClient, byIdCommand, source, out byIdOutput, out byIdError) ||
            TryExecuteCommandForcedSudo(sourceClient, byIdCommand, source, out byIdOutput, out byIdError))
        {
            foreach (var line in (byIdOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = line.Split(new[] { "->" }, StringSplitOptions.None);
                if (parts.Length != 2)
                {
                    continue;
                }

                var left = NormalizeOptionalValue(parts[0]) ?? string.Empty;
                var right = NormalizeOptionalValue(parts[1]) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(right) || !right.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var metadata = Regex.Replace((left + "|" + right).ToLowerInvariant(), "[^0-9a-z]", string.Empty);
                if (normalizedHints.Any(hint => metadata.Contains(hint)))
                {
                    result.Add(right);
                }
            }
        }

        string sysfsOutput;
        string sysfsError;
        const string sysfsCommand =
            "for dev in /sys/class/block/*; do " +
            "name=$(basename \"$dev\"); " +
            "case \"$name\" in loop*|ram*|sr*|fd*) continue ;; esac; " +
            "path=\"/dev/$name\"; " +
            "meta=\"\"; " +
            "for f in \"$dev/device/wwid\" \"$dev/wwid\" \"$dev/device/vpd_pg83\" \"$dev/device/serial\" \"$dev/device/model\" \"$dev/dm/uuid\" \"$dev/dm/name\"; do " +
            "if [ -r \"$f\" ]; then " +
            "v=$(tr -d '\\000\\r\\n\\t ' < \"$f\" 2>/dev/null); " +
            "if [ -n \"$v\" ]; then meta=\"$meta|$v\"; fi; " +
            "fi; " +
            "done; " +
            "printf '%s%s\\n' \"$path\" \"$meta\"; " +
            "done; " +
            "for item in /dev/mapper/* /dev/disk/by-id/* /dev/disk/by-path/*; do " +
            "if [ -e \"$item\" ]; then " +
            "resolved=$(readlink -f \"$item\" 2>/dev/null); " +
            "if [ -n \"$resolved\" ]; then printf '%s|%s\\n' \"$resolved\" \"$item\"; fi; " +
            "fi; " +
            "done";
        if (TryExecuteCommand(sourceClient, sysfsCommand, source, out sysfsOutput, out sysfsError) ||
            TryExecuteCommandForcedSudo(sourceClient, sysfsCommand, source, out sysfsOutput, out sysfsError))
        {
            foreach (var line in (sysfsOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var normalizedLine = NormalizeOptionalValue(line);
                if (string.IsNullOrWhiteSpace(normalizedLine))
                {
                    continue;
                }

                var parts = normalizedLine.Split(new[] { '|' }, StringSplitOptions.None);
                if (parts.Length == 0)
                {
                    continue;
                }

                var path = NormalizeOptionalValue(parts[0]);
                if (string.IsNullOrWhiteSpace(path) || !path.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var metadata = Regex.Replace(normalizedLine.ToLowerInvariant(), "[^0-9a-z]", string.Empty);
                if (lsblkSamples.Count < 20)
                {
                    lsblkSamples.Add(normalizedLine);
                }

                if (normalizedHints.Any(hint => metadata.Contains(hint)))
                {
                    result.Add(path);
                }
            }
        }

        string configfsOutput;
        string configfsError;
        const string configfsCommand =
            "if [ -d /sys/kernel/config/target/vhost ]; then " +
            "for tgt in /sys/kernel/config/target/vhost/naa.*; do " +
            "[ -d \"$tgt\" ] || continue; " +
            "tname=$(basename \"$tgt\"); " +
            "for lun in \"$tgt\"/tpgt_*/lun/lun_*; do " +
            "[ -d \"$lun\" ] || continue; " +
            "for link in \"$lun\"/*; do " +
            "[ -L \"$link\" ] || continue; " +
            "lname=$(basename \"$link\"); " +
            "core=$(readlink -f \"$link\" 2>/dev/null); " +
            "case \"$core\" in */target/core/*/LUN_*) ;; *) continue ;; esac; " +
            "file=$(cat \"$core/info/basic/file_path\" 2>/dev/null | tr -d '\\r\\n'); " +
            "size=$(cat \"$core/info/basic/size\" 2>/dev/null | tr -d '\\r\\n'); " +
            "init=$(cat \"$core/info/basic/init_ctl_str\" 2>/dev/null | tr -d '\\r\\n'); " +
            "printf '%s|%s|%s|%s|%s|%s\\n' \"$tname\" \"$lname\" \"$core\" \"$file\" \"$size\" \"$init\"; " +
            "done; " +
            "done; " +
            "done; " +
            "fi";
        if (TryExecuteCommand(sourceClient, configfsCommand, source, out configfsOutput, out configfsError) ||
            TryExecuteCommandForcedSudo(sourceClient, configfsCommand, source, out configfsOutput, out configfsError))
        {
            foreach (var line in (configfsOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var normalizedLine = NormalizeOptionalValue(line);
                if (string.IsNullOrWhiteSpace(normalizedLine))
                {
                    continue;
                }

                var compactMetadata = Regex.Replace(normalizedLine.ToLowerInvariant(), "[^0-9a-z]", string.Empty);
                if (!normalizedHints.Any(hint => compactMetadata.Contains(hint)))
                {
                    continue;
                }

                if (lsblkSamples.Count < 20)
                {
                    lsblkSamples.Add("cfgfs:" + normalizedLine);
                }

                var parts = normalizedLine.Split(new[] { '|' }, 6);
                if (parts.Length < 4)
                {
                    continue;
                }

                var filePath = NormalizeOptionalValue(parts[3]);
                if (string.IsNullOrWhiteSpace(filePath) && parts.Length >= 6)
                {
                    var initControl = NormalizeOptionalValue(parts[5]) ?? string.Empty;
                    var match = Regex.Match(initControl, "(?:^|,)fd_dev_name=([^,]+)", RegexOptions.IgnoreCase);
                    filePath = match.Success ? NormalizeOptionalValue(match.Groups[1].Value) : null;
                }

                if (string.IsNullOrWhiteSpace(filePath))
                {
                    continue;
                }

                if (!filePath.StartsWith("/", StringComparison.Ordinal))
                {
                    continue;
                }

                result.Add(filePath);
            }
        }

        var resolved = result
            .Where(path => IsPotentialVirtualMachineBlockDevice(path) || IsLikelyVirtualMachineDiskCandidatePath(path))
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (resolved.Count == 0)
        {
            Log("VM block-hint lookup: no block device match. hints={0}; lsblk sample={1}",
                string.Join(", ", normalizedHints.Take(6)),
                lsblkSamples.Count == 0 ? "none" : string.Join(" || ", lsblkSamples));
        }

        return resolved;
    }

    private static string ResolveVirtualMachineProcessId(SshClient sourceClient, ConnectionInfoData source, string uuid, string vmName)
    {
        var tokens = new List<string>();
        if (!string.IsNullOrWhiteSpace(uuid))
        {
            tokens.Add(uuid);
        }

        if (!string.IsNullOrWhiteSpace(vmName))
        {
            tokens.Add(vmName);
        }

        foreach (var token in tokens.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            string output;
            string error;
            var commandText =
                "ps -eo pid,args 2>/dev/null | grep -F " + ShellQuote(token) +
                " | grep -E 'qemu|kvm' | grep -v grep | awk 'NR==1 {print $1}'";
            if (TryExecuteCommand(sourceClient, commandText, source, out output, out error) ||
                TryExecuteCommandForcedSudo(sourceClient, commandText, source, out output, out error))
            {
                var pid = ParseIntValue(output);
                if (pid > 0)
                {
                    return pid.ToString(CultureInfo.InvariantCulture);
                }
            }
        }

        return null;
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksFromQemuMonitor(
        SshClient sourceClient,
        ConnectionInfoData source,
        string vmName)
    {
        if (string.IsNullOrWhiteSpace(vmName))
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var commands = new[]
        {
            "qemu-monitor-command " + ShellQuote(vmName) + " --hmp 'info block'",
            "qemu-monitor-command " + ShellQuote(vmName) + " --hmp \"info block\""
        };

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var command in commands)
        {
            string output;
            if (!TryExecuteSynologyVirshCommand(sourceClient, source, command, out output) ||
                string.IsNullOrWhiteSpace(output))
            {
                continue;
            }

            foreach (var line in output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var trimmed = NormalizeOptionalValue(line);
                if (string.IsNullOrWhiteSpace(trimmed) ||
                    trimmed.IndexOf("not inserted", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    continue;
                }

                ExtractDiskPathCandidatesFromProcessToken(paths, trimmed);
                foreach (Match match in Regex.Matches(trimmed, "(/[^\\s'\",\\)]+)"))
                {
                    AddProcessDiskPathCandidate(paths, match.Groups[1].Value);
                }
            }
        }

        return paths
            .Select((path, index) => new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "mon" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            })
            .ToList();
    }

    private static void AddProcessDiskPathCandidate(HashSet<string> target, string value)
    {
        var normalized = DecodeEscapedPathToken(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return;
        }

        normalized = normalized.Trim('\'', '"');
        if (normalized.StartsWith("pipe:[", StringComparison.OrdinalIgnoreCase) ||
            normalized.StartsWith("socket:[", StringComparison.OrdinalIgnoreCase) ||
            normalized.StartsWith("anon_inode:", StringComparison.OrdinalIgnoreCase) ||
            normalized.IndexOf("anon_inode", StringComparison.OrdinalIgnoreCase) >= 0 ||
            normalized.IndexOf("eventfd", StringComparison.OrdinalIgnoreCase) >= 0 ||
            normalized.IndexOf("eventpoll", StringComparison.OrdinalIgnoreCase) >= 0 ||
            normalized.IndexOf("signalfd", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return;
        }

        if (normalized.StartsWith("/proc/", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!normalized.StartsWith("/", StringComparison.Ordinal))
        {
            return;
        }

        var lowered = normalized.ToLowerInvariant();
        if (lowered.EndsWith(".pid", StringComparison.Ordinal) ||
            lowered.EndsWith(".sock", StringComparison.Ordinal) ||
            lowered.EndsWith(".monitor", StringComparison.Ordinal) ||
            lowered.EndsWith(".xml", StringComparison.Ordinal) ||
            lowered.EndsWith(".json", StringComparison.Ordinal) ||
            lowered.EndsWith(".log", StringComparison.Ordinal))
        {
            return;
        }

        if (normalized.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase) &&
            !IsPotentialVirtualMachineBlockDevice(normalized))
        {
            return;
        }

        if (normalized.EndsWith(".iso", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (IsVirtualMachineAuxiliaryDiskPath(normalized))
        {
            return;
        }

        if (!normalized.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase) &&
            !IsLikelyVirtualMachineDiskCandidatePath(normalized))
        {
            return;
        }

        target.Add(normalized);
    }

    private static void AddProcessConfigPathCandidate(HashSet<string> target, string value)
    {
        var normalized = DecodeEscapedPathToken(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return;
        }

        normalized = normalized.Trim('\'', '"');
        if (!normalized.StartsWith("/", StringComparison.Ordinal))
        {
            return;
        }

        var lowered = normalized.ToLowerInvariant();
        if (!(lowered.EndsWith(".xml", StringComparison.Ordinal) ||
              lowered.EndsWith(".cfg", StringComparison.Ordinal) ||
              lowered.EndsWith(".conf", StringComparison.Ordinal)))
        {
            return;
        }

        target.Add(normalized);
    }

    private static void ExtractDiskPathCandidatesFromProcessToken(HashSet<string> target, string token)
    {
        var line = NormalizeOptionalValue(token);
        if (string.IsNullOrWhiteSpace(line))
        {
            return;
        }

        foreach (Match match in Regex.Matches(line, "(?:^|[;,])file=([^,;\\s]+)", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "filename=([^,;\\s]+)", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "\"(?:filename|path|source|file)\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "'(?:filename|path|source|file)'\\s*:\\s*'((?:\\\\.|[^'])*)'", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "(/[^\\s'\",]+\\.(?:qcow2|img|raw|vmdk|vhd|vhdx|vdi))", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "(/dev/[^\\s'\",;]+)", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }

        foreach (Match match in Regex.Matches(line, "(/volume[^\\s'\",;]+)", RegexOptions.IgnoreCase))
        {
            AddProcessDiskPathCandidate(target, match.Groups[1].Value);
        }
    }

    private static string DecodeEscapedPathToken(string value)
    {
        var normalized = NormalizeOptionalValue(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return null;
        }

        return normalized
            .Replace("\\/", "/")
            .Replace("\\\\", "\\");
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksFromRuntimeConfigFile(
        SshClient sourceClient,
        ConnectionInfoData source,
        string configPath)
    {
        if (string.IsNullOrWhiteSpace(configPath))
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        string output;
        string error;
        var readCommand = "cat " + ShellQuote(configPath);
        if (!(TryExecuteCommand(sourceClient, readCommand, source, out output, out error) ||
              TryExecuteCommandForcedSudo(sourceClient, readCommand, source, out output, out error)))
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        if (string.IsNullOrWhiteSpace(output))
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var result = new List<VirtualMachineDiskDefinition>();
        if (output.IndexOf("<domain", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            MergeVirtualMachineDisks(result, ParseVirtualMachineDisksFromXml(output));
            MergeVirtualMachineDisks(result, ParseVirtualMachineDisksFromXmlCommandLine(output));
        }

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var line in output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            ExtractDiskPathCandidatesFromProcessToken(paths, line);
        }

        MergeVirtualMachineDisks(result, paths
            .Select((path, index) => new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "cfg" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            })
            .ToList());

        return result;
    }

    private static bool IsPotentialVirtualMachineBlockDevice(string path)
    {
        var normalized = NormalizeOptionalValue(path);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        if (!normalized.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var lowered = normalized.ToLowerInvariant();
        if (lowered == "/dev/null" ||
            lowered == "/dev/zero" ||
            lowered == "/dev/random" ||
            lowered == "/dev/urandom" ||
            lowered == "/dev/full" ||
            lowered == "/dev/tty")
        {
            return false;
        }

        if (lowered.StartsWith("/dev/pts/", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/fd/", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/shm/", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/mqueue/", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/sr", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/loop", StringComparison.Ordinal) ||
            lowered.StartsWith("/dev/ram", StringComparison.Ordinal))
        {
            return false;
        }

        return lowered.StartsWith("/dev/iscsi", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/dm-", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/mapper/", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/sd", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/vd", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/xvd", StringComparison.Ordinal) ||
               lowered.StartsWith("/dev/nvme", StringComparison.Ordinal);
    }

    private static bool IsVirtualMachineAuxiliaryDiskPath(string sourcePath)
    {
        var normalized = NormalizeOptionalValue(sourcePath);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        var fileName = NormalizeOptionalValue(Path.GetFileName(normalized) ?? string.Empty);
        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = normalized;
        }

        if (fileName.Equals("-", StringComparison.Ordinal))
        {
            return true;
        }

        if (fileName.EndsWith(".iso", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (fileName.EndsWith(".aes", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".key", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".pem", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (fileName.IndexOf("master-key", StringComparison.OrdinalIgnoreCase) >= 0 ||
            fileName.IndexOf("secret", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (normalized.IndexOf("/var/lib/libvirt/qemu/domain-", StringComparison.OrdinalIgnoreCase) >= 0 &&
            (fileName.IndexOf("master-key", StringComparison.OrdinalIgnoreCase) >= 0 ||
             fileName.EndsWith(".aes", StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        return false;
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksByFilesystem(
        SshClient sourceClient,
        ConnectionInfoData source,
        string uuid,
        string vmName,
        string displayName)
    {
        var paths = DiscoverVirtualMachineDiskPathsByFilesystem(sourceClient, source, uuid, vmName, displayName);
        var result = new List<VirtualMachineDiskDefinition>();
        var index = 0;
        foreach (var path in paths)
        {
            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "fs" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            });
            index++;
        }

        var poolDisks = DiscoverVirtualMachineDisksFromStoragePools(sourceClient, source, uuid, vmName, displayName);
        if (poolDisks.Count > 0)
        {
            MergeVirtualMachineDisks(result, poolDisks);
        }

        var reference = NormalizeOptionalValue(displayName) ??
                        NormalizeOptionalValue(vmName) ??
                        NormalizeOptionalValue(uuid) ??
                        "unknown";
        Log("VM {0}: fallback candidates (filesystem={1}, pools={2}, merged={3}).", reference, paths.Count, poolDisks.Count, result.Count);

        return result;
    }

    private static List<string> DiscoverVirtualMachineDiskPathsByFilesystem(
        SshClient sourceClient,
        ConnectionInfoData source,
        string uuid,
        string vmName,
        string displayName)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tokenCandidates = new List<string>();
        AddVirtualMachineLookupCandidate(tokenCandidates, uuid);
        AddVirtualMachineLookupCandidate(tokenCandidates, vmName);
        AddVirtualMachineLookupCandidate(tokenCandidates, displayName);
        AddVirtualMachineLookupCandidate(
            tokenCandidates,
            Regex.Replace(NormalizeOptionalValue(displayName) ?? string.Empty, "^dsm\\s*instance\\s*:\\s*", string.Empty, RegexOptions.IgnoreCase));

        var uniqueTokens = tokenCandidates
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var token in uniqueTokens)
        {
            foreach (var command in BuildFilesystemDiskSearchCommands(token))
            {
                string output;
                string error;
                var hasAnyCommandSuccess = TryExecuteCommand(sourceClient, command, source, out output, out error);
                var mergedOutput = output ?? string.Empty;

                string sudoOutput;
                string sudoError;
                if (source != null &&
                    source.Password != null &&
                    TryExecuteCommandForcedSudo(sourceClient, command, source, out sudoOutput, out sudoError))
                {
                    hasAnyCommandSuccess = true;
                    if (!string.IsNullOrWhiteSpace(sudoOutput))
                    {
                        if (!string.IsNullOrWhiteSpace(mergedOutput))
                        {
                            mergedOutput += Environment.NewLine;
                        }

                        mergedOutput += sudoOutput;
                    }
                }

                if (!hasAnyCommandSuccess)
                {
                    continue;
                }

                foreach (var line in (mergedOutput ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var path = NormalizeOptionalValue(line);
                    if (string.IsNullOrWhiteSpace(path) ||
                        !path.StartsWith("/", StringComparison.Ordinal) ||
                        IsVirtualMachineAuxiliaryDiskPath(path) ||
                        !IsLikelyVirtualMachineDiskCandidatePath(path))
                    {
                        continue;
                    }

                    result.Add(path);
                }
            }
        }

        var orderedResult = result
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (orderedResult.Count == 0 && uniqueTokens.Count > 0)
        {
            Log("VM filesystem lookup returned no paths: uuid={0}, name={1}, display={2}, tokens={3}",
                NormalizeOptionalValue(uuid) ?? "-",
                NormalizeOptionalValue(vmName) ?? "-",
                NormalizeOptionalValue(displayName) ?? "-",
                string.Join(", ", uniqueTokens));
        }

        return orderedResult;
    }

    private static IEnumerable<string> BuildFilesystemDiskSearchCommands(string token)
    {
        var normalizedToken = NormalizeOptionalValue(token);
        if (string.IsNullOrWhiteSpace(normalizedToken))
        {
            yield break;
        }

        var escaped = ShellQuote(normalizedToken);
        yield return "token=" + escaped + "; " +
                     "for root in /volume*/@GuestImage /volume*/@iSCSI/@VirtualMachine /volume*/@iSCSI/LUN/VDISK_BLUN /var/packages/Virtualization/target/var/lib/libvirt/images /var/lib/libvirt/images; do " +
                     "if [ -d \"$root/$token\" ]; then find \"$root/$token\" -maxdepth 8 \\( -type f -o -type b -o -type l \\) 2>/dev/null; fi; " +
                     "done";

        yield return "token=" + escaped + "; " +
                     "for root in /volume*/@GuestImage /volume*/@iSCSI/@VirtualMachine /volume*/@iSCSI/LUN/VDISK_BLUN /var/packages/Virtualization/target/var/lib/libvirt/images /var/lib/libvirt/images; do " +
                     "if [ -d \"$root\" ]; then find \"$root\" -maxdepth 10 \\( -type f -o -type b -o -type l \\) -ipath \"*$token*\" 2>/dev/null; fi; " +
                     "done";
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksFromStoragePools(
        SshClient sourceClient,
        ConnectionInfoData source,
        string uuid,
        string vmName,
        string displayName)
    {
        var pools = DiscoverSynologyStoragePoolNames(sourceClient, source);
        if (pools.Count == 0)
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var tokens = new List<string>();
        AddVirtualMachineLookupCandidate(tokens, uuid);
        AddVirtualMachineLookupCandidate(tokens, vmName);
        AddVirtualMachineLookupCandidate(tokens, displayName);
        AddVirtualMachineLookupCandidate(
            tokens,
            Regex.Replace(NormalizeOptionalValue(displayName) ?? string.Empty, "^dsm\\s*instance\\s*:\\s*", string.Empty, RegexOptions.IgnoreCase));
        var normalizedTokens = tokens
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Select(item => item.ToLowerInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (normalizedTokens.Count == 0)
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var result = new List<VirtualMachineDiskDefinition>();
        var index = 0;
        foreach (var pool in pools)
        {
            string output;
            if (!TryExecuteSynologyVirshCommand(sourceClient, source, "vol-list --pool " + ShellQuote(pool) + " --details", out output))
            {
                continue;
            }

            foreach (var line in SplitDataLines(output))
            {
                var parts = Regex.Split((line ?? string.Empty).Trim(), "\\s+")
                    .Where(item => !string.IsNullOrWhiteSpace(item))
                    .ToArray();
                if (parts.Length < 2)
                {
                    continue;
                }

                var volumeName = NormalizeOptionalValue(parts[0]);
                var path = NormalizeOptionalValue(parts[1]);
                var searchable = ((volumeName ?? string.Empty) + " " + (path ?? string.Empty)).ToLowerInvariant();
                if (!normalizedTokens.Any(token => searchable.Contains(token)))
                {
                    continue;
                }

                if (IsVirtualMachineAuxiliaryDiskPath(path))
                {
                    continue;
                }

                var disk = new VirtualMachineDiskDefinition
                {
                    DeviceType = "disk",
                    TargetName = "pool" + index.ToString(CultureInfo.InvariantCulture),
                    SourcePool = pool,
                    SourceVolume = volumeName
                };

                if (!string.IsNullOrWhiteSpace(path) &&
                    path.StartsWith("/", StringComparison.Ordinal) &&
                    IsLikelyVirtualMachineDiskCandidatePath(path))
                {
                    disk.SourcePath = path;
                }

                result.Add(disk);
                index++;
            }
        }

        return result;
    }

    private static List<string> DiscoverSynologyStoragePoolNames(SshClient sourceClient, ConnectionInfoData source)
    {
        string output;
        if (!TryExecuteSynologyVirshCommand(sourceClient, source, "pool-list --all --name", out output))
        {
            return new List<string>();
        }

        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(item => NormalizeOptionalValue(item))
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool IsLikelyVirtualMachineDiskCandidatePath(string path)
    {
        var normalized = NormalizeOptionalValue(path);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        if (IsVirtualMachineAuxiliaryDiskPath(normalized))
        {
            return false;
        }

        if (normalized.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
        {
            return IsPotentialVirtualMachineBlockDevice(normalized);
        }

        var extension = NormalizeOptionalValue(Path.GetExtension(normalized) ?? string.Empty) ?? string.Empty;
        extension = extension.ToLowerInvariant();
        var knownDiskExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            ".qcow2", ".qcow", ".img", ".raw", ".vmdk", ".vhd", ".vhdx", ".vdi"
        };
        if (knownDiskExtensions.Contains(extension))
        {
            return true;
        }

        var knownNonDiskExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            ".xml", ".txt", ".json", ".conf", ".cfg", ".ini", ".log", ".pid", ".lock", ".sha", ".sig", ".bak"
        };
        if (knownNonDiskExtensions.Contains(extension))
        {
            return false;
        }

        var name = NormalizeOptionalValue(Path.GetFileNameWithoutExtension(normalized) ?? string.Empty) ?? string.Empty;
        if (name.IndexOf("disk", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("vdisk", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("volume", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("image", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("drive", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("system", StringComparison.OrdinalIgnoreCase) >= 0 ||
            name.IndexOf("data", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (normalized.IndexOf("/@GuestImage/", StringComparison.OrdinalIgnoreCase) >= 0 &&
            extension.Length == 0)
        {
            return true;
        }

        if (normalized.IndexOf("/@iSCSI/@VirtualMachine/", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (normalized.IndexOf("/@iSCSI/LUN/VDISK_BLUN/", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (normalized.IndexOf("/libvirt/images/", StringComparison.OrdinalIgnoreCase) >= 0 &&
            !knownNonDiskExtensions.Contains(extension))
        {
            return true;
        }

        return false;
    }

    private static List<VirtualMachineDiskDefinition> ParseVirtualMachineDisksFromXmlCommandLine(string xml)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        var commandBlock = GetRegexGroupValueSingleline(xml, "<qemu:commandline>(.*?)</qemu:commandline>", 1);
        if (string.IsNullOrWhiteSpace(commandBlock))
        {
            return result;
        }

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in Regex.Matches(commandBlock, "value=['\"]([^'\"]+)['\"]", RegexOptions.IgnoreCase))
        {
            var value = NormalizeOptionalValue(match.Groups[1].Value);
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            foreach (Match fileMatch in Regex.Matches(value, "(?:^|[;,])file=([^,;\\s]+)", RegexOptions.IgnoreCase))
            {
                AddProcessDiskPathCandidate(paths, fileMatch.Groups[1].Value);
            }

            foreach (Match fileMatch in Regex.Matches(value, "filename=([^,;\\s]+)", RegexOptions.IgnoreCase))
            {
                AddProcessDiskPathCandidate(paths, fileMatch.Groups[1].Value);
            }

            foreach (Match pathMatch in Regex.Matches(value, "(/[^\\s'\",]+\\.(?:qcow2|img|raw|vmdk|vhd|vhdx|vdi))", RegexOptions.IgnoreCase))
            {
                AddProcessDiskPathCandidate(paths, pathMatch.Groups[1].Value);
            }
        }

        var index = 0;
        foreach (var path in paths)
        {
            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "cmd" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            });
            index++;
        }

        return result;
    }

    private static List<VirtualMachineDiskDefinition> ParseVirtualMachineDisksFromXml(string xml)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        var index = 0;
        foreach (Match match in Regex.Matches(xml ?? string.Empty, "<disk\\b[\\s\\S]*?</disk>", RegexOptions.IgnoreCase))
        {
            var block = match.Value;
            var deviceType = GetRegexGroupValue(block, "<disk[^>]+device=['\"]([^'\"]+)['\"]", 1);
            if (string.Equals(deviceType, "cdrom", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(deviceType, "floppy", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var targetName = GetRegexGroupValue(block, "<target[^>]+dev=['\"]([^'\"]+)['\"]", 1);
            var sourcePath =
                GetRegexGroupValue(block, "<source[^>]+file=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+dev=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+path=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+dir=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+name=['\"]([^'\"]+)['\"]", 1);
            var sourcePool = GetRegexGroupValue(block, "<source[^>]+pool=['\"]([^'\"]+)['\"]", 1);
            var sourceVolume = GetRegexGroupValue(block, "<source[^>]+volume=['\"]([^'\"]+)['\"]", 1);
            var sourceProtocol = GetRegexGroupValue(block, "<source[^>]+protocol=['\"]([^'\"]+)['\"]", 1);

            if (!string.IsNullOrWhiteSpace(sourceProtocol) &&
                !string.IsNullOrWhiteSpace(sourcePath) &&
                !sourcePath.StartsWith("/", StringComparison.Ordinal) &&
                sourcePath.IndexOf("://", StringComparison.OrdinalIgnoreCase) < 0)
            {
                sourcePath = sourceProtocol + "://" + sourcePath;
            }

            if (string.IsNullOrWhiteSpace(sourcePool) &&
                string.IsNullOrWhiteSpace(sourceVolume) &&
                !string.IsNullOrWhiteSpace(sourcePath) &&
                !sourcePath.StartsWith("/", StringComparison.Ordinal) &&
                sourcePath.Contains("/"))
            {
                var slashIndex = sourcePath.IndexOf('/');
                if (slashIndex > 0 && slashIndex < sourcePath.Length - 1)
                {
                    sourcePool = sourcePath.Substring(0, slashIndex);
                    sourceVolume = sourcePath.Substring(slashIndex + 1);
                }
            }

            if (string.IsNullOrWhiteSpace(targetName))
            {
                targetName = "disk" + index.ToString(CultureInfo.InvariantCulture);
            }

            if ((string.IsNullOrWhiteSpace(sourcePath) &&
                 (string.IsNullOrWhiteSpace(sourcePool) || string.IsNullOrWhiteSpace(sourceVolume))))
            {
                index++;
                continue;
            }

            if (IsVirtualMachineAuxiliaryDiskPath(sourcePath))
            {
                index++;
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = string.IsNullOrWhiteSpace(deviceType) ? "disk" : deviceType,
                TargetName = targetName,
                SourcePath = sourcePath,
                SourcePool = sourcePool,
                SourceVolume = sourceVolume,
                Format = GetRegexGroupValue(block, "<driver[^>]+type=['\"]([^'\"]+)['\"]", 1)
            });
            index++;
        }

        return result
            .GroupBy(item => item.TargetName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(item => item.TargetName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static void MergeVirtualMachineDisks(List<VirtualMachineDiskDefinition> target, List<VirtualMachineDiskDefinition> source)
    {
        foreach (var sourceDisk in source ?? new List<VirtualMachineDiskDefinition>())
        {
            if (sourceDisk == null)
            {
                continue;
            }

            var existing = target.FirstOrDefault(item => string.Equals(item.TargetName, sourceDisk.TargetName, StringComparison.OrdinalIgnoreCase));
            if (existing == null && !string.IsNullOrWhiteSpace(sourceDisk.SourcePath))
            {
                existing = target.FirstOrDefault(item =>
                    !string.IsNullOrWhiteSpace(item.SourcePath) &&
                    string.Equals(item.SourcePath, sourceDisk.SourcePath, StringComparison.OrdinalIgnoreCase));
            }

            if (existing == null &&
                !string.IsNullOrWhiteSpace(sourceDisk.SourcePool) &&
                !string.IsNullOrWhiteSpace(sourceDisk.SourceVolume))
            {
                existing = target.FirstOrDefault(item =>
                    string.Equals(item.SourcePool ?? string.Empty, sourceDisk.SourcePool, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(item.SourceVolume ?? string.Empty, sourceDisk.SourceVolume, StringComparison.OrdinalIgnoreCase));
            }

            if (existing == null &&
                target.Count == 1 &&
                string.IsNullOrWhiteSpace(sourceDisk.SourcePath) &&
                string.IsNullOrWhiteSpace(sourceDisk.SourcePool) &&
                string.IsNullOrWhiteSpace(sourceDisk.SourceVolume))
            {
                existing = target[0];
            }

            if (existing == null)
            {
                target.Add(sourceDisk);
                continue;
            }

            if (string.IsNullOrWhiteSpace(existing.SourcePath))
            {
                existing.SourcePath = sourceDisk.SourcePath;
            }

            if (string.IsNullOrWhiteSpace(existing.Format))
            {
                existing.Format = sourceDisk.Format;
            }

            if (string.IsNullOrWhiteSpace(existing.SourcePool))
            {
                existing.SourcePool = sourceDisk.SourcePool;
            }

            if (string.IsNullOrWhiteSpace(existing.SourceVolume))
            {
                existing.SourceVolume = sourceDisk.SourceVolume;
            }

            if (existing.VirtualSizeBytes <= 0 && sourceDisk.VirtualSizeBytes > 0)
            {
                existing.VirtualSizeBytes = sourceDisk.VirtualSizeBytes;
            }

            if (existing.ActualSizeBytes <= 0 && sourceDisk.ActualSizeBytes > 0)
            {
                existing.ActualSizeBytes = sourceDisk.ActualSizeBytes;
            }

            if (existing.ActualSizeBytes <= 0 && existing.VirtualSizeBytes > 0)
            {
                existing.ActualSizeBytes = existing.VirtualSizeBytes;
            }
        }
    }

    private static bool HasUsableVirtualMachineDisks(List<VirtualMachineDiskDefinition> disks)
    {
        return disks != null &&
               disks.Any(item =>
                   item != null &&
                   ((!string.IsNullOrWhiteSpace(item.SourcePath) && !IsVirtualMachineAuxiliaryDiskPath(item.SourcePath)) ||
                    (!string.IsNullOrWhiteSpace(item.SourcePool) && !string.IsNullOrWhiteSpace(item.SourceVolume)) ||
                    item.VirtualSizeBytes > 0 ||
                    item.ActualSizeBytes > 0));
    }

    private static void PopulateVirtualMachineDiskInfoFromDomBlkInfo(SshClient sourceClient, ConnectionInfoData source, string vmName, VirtualMachineDiskDefinition disk)
    {
        if (disk == null || string.IsNullOrWhiteSpace(vmName) || string.IsNullOrWhiteSpace(disk.TargetName))
        {
            return;
        }

        try
        {
            var output = ExecuteSynologyVirshCommand(sourceClient, source, "domblkinfo " + ShellQuote(vmName) + " " + ShellQuote(disk.TargetName));
            var values = ParseKeyValueOutput(output);
            var capacity = ParseLongValue(GetDictionaryValue(values, "Capacity"));
            var allocation = ParseLongValue(GetDictionaryValue(values, "Allocation"));
            var physical = ParseLongValue(GetDictionaryValue(values, "Physical"));

            if (disk.VirtualSizeBytes <= 0 && capacity > 0)
            {
                disk.VirtualSizeBytes = capacity;
            }

            if (disk.ActualSizeBytes <= 0)
            {
                if (allocation > 0)
                {
                    disk.ActualSizeBytes = allocation;
                }
                else if (physical > 0)
                {
                    disk.ActualSizeBytes = physical;
                }
            }

            if (disk.ActualSizeBytes <= 0 && disk.VirtualSizeBytes > 0)
            {
                disk.ActualSizeBytes = disk.VirtualSizeBytes;
            }
        }
        catch
        {
        }
    }

    private static void EnsureVirtualMachineDiskSourcePath(SshClient sourceClient, ConnectionInfoData source, string vmName, VirtualMachineDiskDefinition disk)
    {
        if (disk == null)
        {
            return;
        }

        var currentPath = NormalizeOptionalValue(disk.SourcePath);
        if (!string.IsNullOrWhiteSpace(currentPath) && RemotePathExists(sourceClient, source, currentPath))
        {
            disk.SourcePath = currentPath;
            return;
        }

        var sourcePool = NormalizeOptionalValue(disk.SourcePool);
        var sourceVolume = NormalizeOptionalValue(disk.SourceVolume);
        if ((string.IsNullOrWhiteSpace(sourcePool) || string.IsNullOrWhiteSpace(sourceVolume)) &&
            !string.IsNullOrWhiteSpace(currentPath) &&
            !currentPath.StartsWith("/") &&
            currentPath.Contains("/"))
        {
            var slashIndex = currentPath.IndexOf('/');
            if (slashIndex > 0 && slashIndex < currentPath.Length - 1)
            {
                sourcePool = currentPath.Substring(0, slashIndex);
                sourceVolume = currentPath.Substring(slashIndex + 1);
            }
        }

        if (string.IsNullOrWhiteSpace(sourceVolume) && !string.IsNullOrWhiteSpace(currentPath) && !currentPath.StartsWith("/"))
        {
            sourceVolume = currentPath;
        }
        else if (string.IsNullOrWhiteSpace(sourceVolume) && !string.IsNullOrWhiteSpace(currentPath) && currentPath.StartsWith("/"))
        {
            var fileName = NormalizeOptionalValue(Path.GetFileName(currentPath));
            if (!string.IsNullOrWhiteSpace(fileName) && !string.Equals(fileName, "-", StringComparison.Ordinal))
            {
                sourceVolume = fileName;
            }
        }

        var resolved = ResolveVirtualMachineDiskSourcePathFromPoolVolume(sourceClient, source, sourcePool, sourceVolume);
        if (string.IsNullOrWhiteSpace(resolved))
        {
            resolved = ResolveVirtualMachineDiskSourcePathFromDomBlkList(sourceClient, source, vmName, disk.TargetName);
        }

        if (string.IsNullOrWhiteSpace(resolved))
        {
            resolved = ResolveVirtualMachineDiskSourcePathByVolumeName(sourceClient, source, sourceVolume);
        }

        if (string.IsNullOrWhiteSpace(resolved))
        {
            return;
        }

        disk.SourcePath = resolved;
        if (string.IsNullOrWhiteSpace(disk.SourceVolume))
        {
            disk.SourceVolume = sourceVolume;
        }
    }

    private static string ResolveVirtualMachineDiskSourcePathFromPoolVolume(SshClient sourceClient, ConnectionInfoData source, string pool, string volume)
    {
        if (string.IsNullOrWhiteSpace(pool) || string.IsNullOrWhiteSpace(volume))
        {
            return null;
        }

        try
        {
            var output = ExecuteSynologyVirshCommand(sourceClient, source, "vol-path --pool " + ShellQuote(pool) + " " + ShellQuote(volume));
            var resolved = NormalizeOptionalValue((output ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault());
            return string.IsNullOrWhiteSpace(resolved) ? null : resolved;
        }
        catch
        {
            return null;
        }
    }

    private static string ResolveVirtualMachineDiskSourcePathFromDomBlkList(SshClient sourceClient, ConnectionInfoData source, string vmName, string targetName)
    {
        if (string.IsNullOrWhiteSpace(vmName) || string.IsNullOrWhiteSpace(targetName))
        {
            return null;
        }

        try
        {
            var output = ExecuteSynologyVirshCommand(sourceClient, source, "domblklist --details " + ShellQuote(vmName));
            var disks = ParseVirtualMachineDisks(output);
            var match = disks.FirstOrDefault(item => string.Equals(item.TargetName, targetName, StringComparison.OrdinalIgnoreCase));
            if (match != null && !string.IsNullOrWhiteSpace(match.SourcePath) && !string.Equals(match.SourcePath, "-", StringComparison.Ordinal))
            {
                return match.SourcePath;
            }
        }
        catch
        {
        }

        return null;
    }

    private static string ResolveVirtualMachineDiskSourcePathByVolumeName(SshClient sourceClient, ConnectionInfoData source, string volume)
    {
        if (string.IsNullOrWhiteSpace(volume))
        {
            return null;
        }

        try
        {
            var poolsOutput = ExecuteSynologyVirshCommand(sourceClient, source, "pool-list --all --name");
            var pools = (poolsOutput ?? string.Empty)
                .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(item => NormalizeOptionalValue(item))
                .Where(item => !string.IsNullOrWhiteSpace(item))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var pool in pools)
            {
                var resolved = ResolveVirtualMachineDiskSourcePathFromPoolVolume(sourceClient, source, pool, volume);
                if (!string.IsNullOrWhiteSpace(resolved))
                {
                    return resolved;
                }
            }
        }
        catch
        {
        }

        return null;
    }

    private static void PopulateVirtualMachineDiskInfoFromVolumeInfo(SshClient sourceClient, ConnectionInfoData source, VirtualMachineDiskDefinition disk)
    {
        if (disk == null)
        {
            return;
        }

        var pool = NormalizeOptionalValue(disk.SourcePool);
        var volume = NormalizeOptionalValue(disk.SourceVolume);
        if (string.IsNullOrWhiteSpace(pool) || string.IsNullOrWhiteSpace(volume))
        {
            return;
        }

        try
        {
            var output = ExecuteSynologyVirshCommand(sourceClient, source, "vol-info --pool " + ShellQuote(pool) + " " + ShellQuote(volume) + " --bytes");
            var values = ParseKeyValueOutput(output);
            var capacity = ParseLongValue(GetDictionaryValue(values, "Capacity"));
            var allocation = ParseLongValue(GetDictionaryValue(values, "Allocation"));

            if (disk.VirtualSizeBytes <= 0 && capacity > 0)
            {
                disk.VirtualSizeBytes = capacity;
            }

            if (disk.ActualSizeBytes <= 0 && allocation > 0)
            {
                disk.ActualSizeBytes = allocation;
            }

            if (disk.ActualSizeBytes <= 0 && disk.VirtualSizeBytes > 0)
            {
                disk.ActualSizeBytes = disk.VirtualSizeBytes;
            }
        }
        catch
        {
        }
    }

    private static List<VirtualMachineInterfaceDefinition> ParseVirtualMachineInterfaces(string output)
    {
        var result = new List<VirtualMachineInterfaceDefinition>();
        foreach (var line in SplitDataLines(output))
        {
            var parts = Regex.Split(line.Trim(), "\\s+").Where(item => item.Length > 0).ToArray();
            if (parts.Length < 5)
            {
                continue;
            }

            if (string.Equals(parts[0], "Interface", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(parts[1], "Type", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            result.Add(new VirtualMachineInterfaceDefinition
            {
                InterfaceName = parts[0],
                Type = parts[1],
                SourceName = parts[2],
                Model = parts[3],
                MacAddress = parts[4]
            });
        }

        return result;
    }

    private static List<VirtualMachineInterfaceDefinition> ParseVirtualMachineInterfacesFromXml(string xml)
    {
        var result = new List<VirtualMachineInterfaceDefinition>();
        foreach (Match match in Regex.Matches(xml ?? string.Empty, "<interface\\b[\\s\\S]*?</interface>", RegexOptions.IgnoreCase))
        {
            var block = match.Value;
            var interfaceType = GetRegexGroupValue(block, "<interface[^>]+type=['\"]([^'\"]+)['\"]", 1);
            var sourceName =
                GetRegexGroupValue(block, "<source[^>]+bridge=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+network=['\"]([^'\"]+)['\"]", 1) ??
                GetRegexGroupValue(block, "<source[^>]+dev=['\"]([^'\"]+)['\"]", 1);
            var model = GetRegexGroupValue(block, "<model[^>]+type=['\"]([^'\"]+)['\"]", 1);
            var mac = GetRegexGroupValue(block, "<mac[^>]+address=['\"]([^'\"]+)['\"]", 1);
            var targetName = GetRegexGroupValue(block, "<target[^>]+dev=['\"]([^'\"]+)['\"]", 1);

            if (string.IsNullOrWhiteSpace(sourceName) && string.IsNullOrWhiteSpace(mac) && string.IsNullOrWhiteSpace(targetName))
            {
                continue;
            }

            result.Add(new VirtualMachineInterfaceDefinition
            {
                InterfaceName = targetName ?? "nic" + result.Count.ToString(CultureInfo.InvariantCulture),
                Type = interfaceType,
                SourceName = sourceName,
                Model = model,
                MacAddress = mac
            });
        }

        return result;
    }

    private static void PopulateVirtualMachineDiskInfo(SshClient sourceClient, ConnectionInfoData source, VirtualMachineDiskDefinition disk, string vmName)
    {
        EnsureVirtualMachineDiskSourcePath(sourceClient, source, vmName, disk);
        PopulateVirtualMachineDiskInfoFromVolumeInfo(sourceClient, source, disk);

        if (string.IsNullOrWhiteSpace(disk.SourcePath))
        {
            return;
        }

        string output;
        string error;
        if (TryExecuteCommand(sourceClient, "qemu-img info --output json " + ShellQuote(disk.SourcePath), source, out output, out error) ||
            TryExecuteCommandForcedSudo(sourceClient, "qemu-img info --output json " + ShellQuote(disk.SourcePath), source, out output, out error))
        {
            try
            {
                var info = Json.DeserializeObject(output) as Dictionary<string, object>;
                if (info != null)
                {
                    disk.Format = GetString(info, "format");
                    disk.VirtualSizeBytes = ParseLongValue(GetString(info, "virtual-size"));
                    disk.ActualSizeBytes = ParseLongValue(GetString(info, "actual-size"));
                }
            }
            catch
            {
            }
        }

        if (disk.VirtualSizeBytes <= 0)
        {
            disk.VirtualSizeBytes = GetVirtualMachineDiskVirtualSizeBytes(sourceClient, source, disk.SourcePath);
        }

        if (disk.ActualSizeBytes <= 0)
        {
            disk.ActualSizeBytes = GetVirtualMachineDiskActualSizeBytes(sourceClient, source, disk.SourcePath);
        }

        if (disk.ActualSizeBytes <= 0 && disk.VirtualSizeBytes > 0)
        {
            disk.ActualSizeBytes = disk.VirtualSizeBytes;
        }

        if (string.IsNullOrWhiteSpace(disk.Format))
        {
            var extension = NormalizeOptionalValue(Path.GetExtension(disk.SourcePath) ?? string.Empty);
            if (!string.IsNullOrWhiteSpace(extension))
            {
                disk.Format = extension.TrimStart('.');
            }
            else if (!string.IsNullOrWhiteSpace(disk.SourcePath) &&
                     disk.SourcePath.StartsWith("/dev/", StringComparison.OrdinalIgnoreCase))
            {
                disk.Format = "raw";
            }
            else
            {
                disk.Format = "img";
            }
        }
    }

    private static List<ProxmoxVirtualMachineDefinition> ParseProxmoxVirtualMachineList(string output)
    {
        var result = new List<ProxmoxVirtualMachineDefinition>();
        foreach (var line in (output ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0 || trimmed.StartsWith("VMID", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var parts = Regex.Split(trimmed, "\\s+").Where(item => item.Length > 0).ToArray();
            if (parts.Length < 6)
            {
                continue;
            }

            int vmId;
            if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out vmId))
            {
                continue;
            }

            result.Add(new ProxmoxVirtualMachineDefinition
            {
                VmId = vmId,
                Name = parts[1],
                Status = parts[2],
                Running = string.Equals(parts[2], "running", StringComparison.OrdinalIgnoreCase),
                MemoryMb = ParseLongValue(parts[3]),
                Cores = ParseIntValue(parts[4])
            });
        }

        return result;
    }

    private static void ApplyProxmoxVirtualMachineConfig(ProxmoxVirtualMachineDefinition vm, string configText)
    {
        vm.RawConfigLines = (configText ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .ToList();

        foreach (var line in vm.RawConfigLines)
        {
            var separatorIndex = line.IndexOf(':');
            if (separatorIndex <= 0)
            {
                continue;
            }

            var key = line.Substring(0, separatorIndex).Trim();
            var value = line.Substring(separatorIndex + 1).Trim();
            if (Regex.IsMatch(key, "^net\\d+$", RegexOptions.IgnoreCase))
            {
                var bridgeMatch = Regex.Match(value, "bridge=([^,]+)", RegexOptions.IgnoreCase);
                vm.Networks.Add(bridgeMatch.Success ? bridgeMatch.Groups[1].Value.Trim() : value);
                continue;
            }

            if (string.Equals(key, "memory", StringComparison.OrdinalIgnoreCase))
            {
                vm.MemoryMb = ParseLongValue(value);
            }
            else if (string.Equals(key, "cores", StringComparison.OrdinalIgnoreCase))
            {
                vm.Cores = ParseIntValue(value);
            }
            else if (string.Equals(key, "name", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(value))
            {
                vm.Name = value;
            }
        }

        vm.Networks = vm.Networks
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<string> ParseProxmoxStorageNames(string output)
    {
        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("Name", StringComparison.OrdinalIgnoreCase))
            .Select(line => Regex.Split(line, "\\s+").FirstOrDefault() ?? string.Empty)
            .Where(line => line.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(line => line, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static VirtualMachineMigrationPlan BuildVirtualMachineMigrationPlan(SshClient sourceClient, SshClient targetClient, VirtualMachineMigrationOptions options)
    {
        var names = (options.VirtualMachineNames ?? new List<string>())
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (names.Count == 0)
        {
            throw new InvalidOperationException("No virtual machines selected for migration.");
        }

        if (string.IsNullOrWhiteSpace(options.TargetStorage))
        {
            throw new InvalidOperationException("Proxmox target storage is required for VM migration.");
        }

        if (string.IsNullOrWhiteSpace(options.TargetBridge))
        {
            throw new InvalidOperationException("Proxmox target bridge is required for VM migration.");
        }

        var definitions = new List<VirtualMachineDefinition>();
        foreach (var name in names)
        {
            VirtualMachineDefinition definition;
            if (!TryInspectVirtualMachineDefinition(sourceClient, options.Source, name, out definition))
            {
                throw new InvalidOperationException("Unable to inspect selected VM: " + name);
            }

            definitions.Add(definition);
        }

        var withoutDisks = definitions
            .Where(item => item != null && (item.Disks == null || item.Disks.Count == 0))
            .Select(item => item.DisplayName ?? item.Name)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (withoutDisks.Count > 0)
        {
            throw new InvalidOperationException(
                "Disk detection failed for selected VM(s): " + string.Join(", ", withoutDisks) + ". " +
                "Migration is blocked to avoid creating diskless VMs on Proxmox.");
        }

        return new VirtualMachineMigrationPlan
        {
            VirtualMachines = definitions,
            TargetStorage = options.TargetStorage.Trim(),
            TargetBridge = options.TargetBridge.Trim()
        };
    }

    private static void LogVirtualMachinePlan(VirtualMachineMigrationPlan plan, VirtualMachineMigrationOptions options)
    {
        Log("Mode: {0}", options.DryRun ? "dry run (plan only)" : "live VM migration");
        Log("Target root: {0}", options.TargetRoot);
        Log("Target storage: {0}", plan.TargetStorage);
        Log("Target bridge: {0}", plan.TargetBridge);
        Log("Virtual machines in plan: {0}", plan.VirtualMachines.Count);
        Log("Estimated VM transfer size: {0}", FormatBytes(plan.VirtualMachines.Sum(item => item.EstimatedTransferBytes)));
        Log(options.StopVirtualMachinesDuringExport
            ? "Source virtual machines will be temporarily stopped during export."
            : "Source virtual machines will not be stopped automatically.");
        Log(options.StartImportedVirtualMachines
            ? "Imported virtual machines will be started on Proxmox after import."
            : "Imported virtual machines will remain stopped on Proxmox after import.");

        foreach (var vm in plan.VirtualMachines)
        {
            Log("Plan VM {0}: state={1}; vcpus={2}; memory={3} GiB; disks={4}; nics={5}; size={6}",
                vm.Name,
                vm.State,
                vm.Vcpus,
                (vm.MemoryKiB / 1024D / 1024D).ToString("0.00", CultureInfo.InvariantCulture),
                vm.Disks.Count,
                vm.Interfaces.Count,
                FormatBytes(vm.EstimatedTransferBytes));

            foreach (var disk in vm.Disks)
            {
                var source = !string.IsNullOrWhiteSpace(disk.SourcePath)
                    ? disk.SourcePath
                    : (!string.IsNullOrWhiteSpace(disk.SourcePool) && !string.IsNullOrWhiteSpace(disk.SourceVolume)
                        ? disk.SourcePool + "/" + disk.SourceVolume
                        : "-");
                var sizeBytes = disk.ActualSizeBytes > 0 ? disk.ActualSizeBytes : disk.VirtualSizeBytes;
                Log("  disk {0}: {1}; format={2}; size={3}", disk.TargetName, source, disk.Format ?? "unknown", FormatBytes(sizeBytes));
            }
        }
    }

    private static void WaitForVirtualMachinePowerState(SshClient sourceClient, ConnectionInfoData source, string vmName, bool shouldBeRunning, int timeoutSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            var info = ParseKeyValueOutput(ExecuteSynologyVirshCommand(sourceClient, source, "dominfo " + ShellQuote(vmName)));
            var running = string.Equals(GetDictionaryValue(info, "State"), "running", StringComparison.OrdinalIgnoreCase);
            if (running == shouldBeRunning)
            {
                return;
            }

            Thread.Sleep(2000);
        }

        throw new InvalidOperationException("Timed out while waiting for VM " + vmName + " to reach power state " + (shouldBeRunning ? "running" : "stopped") + ".");
    }

    private static bool TryExecuteSynologyVirshCommand(SshClient client, ConnectionInfoData connectionInfo, string arguments, out string output)
    {
        output = string.Empty;
        string error;
        var systemCommand = "virsh -c qemu:///system " + arguments;
        if (TryExecuteCommand(client, systemCommand, connectionInfo, out output, out error))
        {
            return true;
        }

        var plainCommand = "virsh " + arguments;
        if (TryExecuteCommand(client, plainCommand, connectionInfo, out output, out error))
        {
            return true;
        }

        if (TryExecuteCommandForcedSudo(client, systemCommand, connectionInfo, out output, out error))
        {
            return true;
        }

        return false;
    }

    private static string ExecuteSynologyVirshCommand(SshClient client, ConnectionInfoData connectionInfo, string arguments)
    {
        string output;
        string error;
        var systemCommand = "virsh -c qemu:///system " + arguments;
        if (TryExecuteCommand(client, systemCommand, connectionInfo, out output, out error))
        {
            return output;
        }

        var failureDetails = BuildRemoteFailureDetails(output, error);
        var plainCommand = "virsh " + arguments;
        if (TryExecuteCommand(client, plainCommand, connectionInfo, out output, out error))
        {
            return output;
        }

        var plainFailureDetails = BuildRemoteFailureDetails(output, error);
        if (TryExecuteCommandForcedSudo(client, systemCommand, connectionInfo, out output, out error))
        {
            Log("VM command required sudo on {0}.", connectionInfo.Host);
            return output;
        }

        var sudoFailureDetails = BuildRemoteFailureDetails(output, error);
        throw new InvalidOperationException(
            "Synology virsh command failed: " + arguments + Environment.NewLine +
            "qemu:///system: " + failureDetails + Environment.NewLine +
            "default connection: " + plainFailureDetails + Environment.NewLine +
            "sudo qemu:///system: " + sudoFailureDetails);
    }

    private static int GetNextProxmoxVmId(SshClient targetClient, ConnectionInfoData target)
    {
        string output;
        string error;
        if (TryExecuteCommand(targetClient, "pvesh get /cluster/nextid", target, out output, out error))
        {
            return ParseIntValue(output);
        }

        var existing = ParseProxmoxVirtualMachineList(ExecuteCommand(targetClient, "qm list", target));
        return existing.Count == 0 ? 100 : existing.Max(item => item.VmId) + 1;
    }

    private static void CreateTargetVirtualMachineSkeleton(SshClient targetClient, ConnectionInfoData target, VirtualMachineDefinition vm, int vmId, string bridge)
    {
        var targetVmName = NormalizeOptionalValue(vm.DisplayName) ?? vm.Name;
        var parts = new List<string>
        {
            "qm create " + vmId.ToString(CultureInfo.InvariantCulture),
            "--name " + ShellQuote(PathToken(targetVmName).ToLowerInvariant()),
            "--memory " + Math.Max(256L, vm.MemoryKiB / 1024L).ToString(CultureInfo.InvariantCulture),
            "--cores " + Math.Max(1, vm.Vcpus).ToString(CultureInfo.InvariantCulture),
            "--sockets 1",
            "--scsihw virtio-scsi-pci"
        };

        if (vm.Autostart)
        {
            parts.Add("--onboot 1");
        }

        if (vm.UsesUefi)
        {
            parts.Add("--bios ovmf");
        }

        if (!string.IsNullOrWhiteSpace(vm.MachineType) && vm.MachineType.IndexOf("q35", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            parts.Add("--machine q35");
        }

        foreach (var nic in vm.Interfaces.Select((item, index) => new { Item = item, Index = index }))
        {
            var model = NormalizeProxmoxNicModel(nic.Item.Model);
            var netValue = model + (string.IsNullOrWhiteSpace(nic.Item.MacAddress) ? string.Empty : "=" + nic.Item.MacAddress) + ",bridge=" + bridge;
            parts.Add("--net" + nic.Index.ToString(CultureInfo.InvariantCulture) + " " + ShellQuote(netValue));
        }

        ExecuteCommand(targetClient, string.Join(" ", parts), target);
    }

    private static void ImportVirtualMachineDisks(SshClient sourceClient, ConnectionInfoData source, SshClient targetClient, ConnectionInfoData target, SftpClient targetSftp, string targetRoot, string stagingRoot, VirtualMachineDefinition vm, string targetStorage)
    {
        ExecuteCommand(targetClient, "mkdir -p " + ShellQuote(targetRoot.TrimEnd('/') + "/vm-import"), target);

        for (var index = 0; index < vm.Disks.Count; index++)
        {
            var disk = vm.Disks[index];
            EnsureVirtualMachineDiskSourcePath(sourceClient, source, vm.Name, disk);
            if (string.IsNullOrWhiteSpace(disk.SourcePath))
            {
                throw new InvalidOperationException(
                    "Unable to resolve source path for VM disk " + vm.Name + ":" + (disk.TargetName ?? ("disk" + index.ToString(CultureInfo.InvariantCulture))) +
                    ". Check libvirt storage pool mapping (pool/volume) and VM disk definition.");
            }

            if (!RemotePathExists(sourceClient, source, disk.SourcePath))
            {
                throw new InvalidOperationException("Source VM disk path does not exist: " + disk.SourcePath);
            }

            Log("Exporting VM disk {0}:{1}", vm.Name, disk.TargetName);
            var localTempPath = DownloadCommandOutputToTempFile(sourceClient, source, "cat " + ShellQuote(disk.SourcePath), BuildVirtualMachineDiskFileName(vm, disk, index));
            try
            {
                var remoteFileName = BuildVirtualMachineDiskFileName(vm, disk, index);
                var remotePath = targetRoot.TrimEnd('/') + "/vm-import/" + remoteFileName;
                Log("Uploading VM disk {0} to Proxmox staging...", disk.TargetName);
                UploadLocalFile(targetSftp, localTempPath, remotePath);

                var vmId = vm.AssignedTargetVmId ?? 0;
                Log("Importing VM disk {0} into Proxmox storage {1}...", disk.TargetName, targetStorage);
                ExecuteCommand(targetClient,
                    "qm importdisk " + vmId.ToString(CultureInfo.InvariantCulture) + " " + ShellQuote(remotePath) + " " + ShellQuote(targetStorage),
                    target);

                var unusedReference = GetLatestUnusedDiskReference(targetClient, target, vmId);
                ExecuteCommand(targetClient,
                    "qm set " + vmId.ToString(CultureInfo.InvariantCulture) + " --scsi" + index.ToString(CultureInfo.InvariantCulture) + " " + ShellQuote(unusedReference),
                    target);

                if (index == 0)
                {
                    ExecuteCommand(targetClient,
                        "qm set " + vmId.ToString(CultureInfo.InvariantCulture) + " --boot order=scsi0",
                        target);
                }

                ExecuteCommand(targetClient, "rm -f " + ShellQuote(remotePath), target);
            }
            finally
            {
                DeleteLocalTempFile(localTempPath);
            }
        }
    }

    private static string GetLatestUnusedDiskReference(SshClient targetClient, ConnectionInfoData target, int vmId)
    {
        var config = ExecuteCommand(targetClient, "qm config " + vmId.ToString(CultureInfo.InvariantCulture), target);
        var match = Regex.Matches(config ?? string.Empty, "^unused\\d+:\\s*(.+)$", RegexOptions.IgnoreCase | RegexOptions.Multiline)
            .Cast<Match>()
            .LastOrDefault(item => item.Success);

        if (match == null)
        {
            throw new InvalidOperationException("Proxmox did not report an unused imported disk for VM " + vmId.ToString(CultureInfo.InvariantCulture) + ".");
        }

        return match.Groups[1].Value.Trim();
    }

    private static string BuildVirtualMachineDiskFileName(VirtualMachineDefinition vm, VirtualMachineDiskDefinition disk, int index)
    {
        var baseName = NormalizeOptionalValue(vm.DisplayName) ?? vm.Name;
        var extension = NormalizeOptionalValue(Path.GetExtension(disk.SourcePath));
        if (string.IsNullOrWhiteSpace(extension))
        {
            extension = "." + (string.IsNullOrWhiteSpace(disk.Format) ? "img" : disk.Format.Trim('.'));
        }

        return "vm-" + PathToken(baseName).ToLowerInvariant() + "-" + index.ToString(CultureInfo.InvariantCulture) + extension;
    }

    private static string NormalizeProxmoxNicModel(string model)
    {
        var normalized = (model ?? string.Empty).Trim().ToLowerInvariant();
        if (normalized == "virtio" || normalized == "e1000" || normalized == "rtl8139" || normalized == "vmxnet3")
        {
            return normalized;
        }

        return "virtio";
    }

    private static Dictionary<string, string> ParseKeyValueOutput(string output)
    {
        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && line.Contains(":"))
            .Select(line =>
            {
                var separatorIndex = line.IndexOf(':');
                return new KeyValuePair<string, string>(
                    line.Substring(0, separatorIndex).Trim(),
                    line.Substring(separatorIndex + 1).Trim());
            })
            .ToDictionary(item => item.Key, item => item.Value, StringComparer.OrdinalIgnoreCase);
    }

    private static IEnumerable<string> SplitDataLines(string output)
    {
        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.TrimEnd())
            .Where(line => line.Trim().Length > 0 &&
                           !line.TrimStart().StartsWith("Type", StringComparison.OrdinalIgnoreCase) &&
                           !line.TrimStart().StartsWith("Target", StringComparison.OrdinalIgnoreCase) &&
                           !line.TrimStart().StartsWith("Id", StringComparison.OrdinalIgnoreCase) &&
                           !line.StartsWith("---", StringComparison.OrdinalIgnoreCase));
    }

    private static List<string> CollectSynologyVirtualMachineNames(SshClient sourceClient, ConnectionInfoData source)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var commands = new[]
        {
            "virsh -c qemu:///system list --all",
            "virsh -c qemu:///system list --all --name",
            "virsh -c qemu:///system list --inactive --name",
            "virsh -c qemu:///system list --state-shutoff --name",
            "virsh list --all",
            "virsh list --all --name",
            "virsh list --inactive --name"
        };

        foreach (var command in commands)
        {
            AddSynologyVirtualMachineNamesFromCommand(sourceClient, source, command, result, false);
            AddSynologyVirtualMachineNamesFromCommand(sourceClient, source, command, result, true);
        }

        return result
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<string> CollectSynologyVirtualMachineNamesFromSynologyApi(SshClient sourceClient, ConnectionInfoData source)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var commands = new[]
        {
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=1",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=2",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=1",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=2",
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=1",
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=1"
        };

        foreach (var command in commands)
        {
            AddSynologyVirtualMachineNamesFromApiCommand(sourceClient, source, command, result, false);
            AddSynologyVirtualMachineNamesFromApiCommand(sourceClient, source, command, result, true);
        }

        if (result.Count > 0)
        {
            Log("Synology API VM identifiers discovered: {0}", result.Count);
        }

        return result
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<VirtualMachineDefinition> DiscoverVirtualMachinesFromSynologyApi(SshClient sourceClient, ConnectionInfoData source)
    {
        var listOutputs = ExecuteSynologyVmApiListOutputs(sourceClient, source);
        var listDefinitions = listOutputs
            .SelectMany(ParseVirtualMachineDefinitionsFromApiOutput)
            .Where(item => item != null && !string.IsNullOrWhiteSpace(item.Uuid))
            .GroupBy(item => item.Uuid, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToDictionary(item => item.Uuid, item => item, StringComparer.OrdinalIgnoreCase);

        var uuids = CollectSynologyVirtualMachineNamesFromSynologyApi(sourceClient, source)
            .Where(item => Regex.IsMatch(item ?? string.Empty, "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var deduped = new List<VirtualMachineDefinition>();
        foreach (var uuid in uuids)
        {
            VirtualMachineDefinition definition;
            if (!listDefinitions.TryGetValue(uuid, out definition))
            {
                if (!TryGetVirtualMachineDefinitionFromSynologyApi(sourceClient, source, uuid, out definition))
                {
                    continue;
                }
            }
            else if (!HasStrongVirtualMachineSignal(definition))
            {
                VirtualMachineDefinition detailedDefinition;
                if (TryGetVirtualMachineDefinitionFromSynologyApi(sourceClient, source, uuid, out detailedDefinition) &&
                    HasStrongVirtualMachineSignal(detailedDefinition))
                {
                    definition = detailedDefinition;
                }
            }

            deduped.Add(definition);
        }

        foreach (var definition in deduped)
        {
            foreach (var disk in definition.Disks ?? new List<VirtualMachineDiskDefinition>())
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, definition.Name ?? definition.Uuid);
            }
        }

        return deduped;
    }

    private static bool HasStrongVirtualMachineSignal(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return false;
        }

        var diskWithSource = definition.Disks == null
            ? null
            : definition.Disks.FirstOrDefault(item =>
                item != null &&
                (!string.IsNullOrWhiteSpace(item.SourcePath) || !string.IsNullOrWhiteSpace(item.SourceVolume)));
        if (diskWithSource != null && !string.IsNullOrWhiteSpace(diskWithSource.TargetName))
        {
            return true;
        }

        if (definition.MemoryKiB > 0 || definition.Vcpus > 0 || (definition.Interfaces != null && definition.Interfaces.Count > 0))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(definition.State) && !string.Equals(definition.State, "unknown", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var hasMeaningfulAlias =
            (!string.IsNullOrWhiteSpace(definition.DisplayName) && !IsLikelyVirtualMachineIdentifier(definition.DisplayName)) ||
            (!string.IsNullOrWhiteSpace(definition.Name) && !IsLikelyVirtualMachineIdentifier(definition.Name));
        return hasMeaningfulAlias;
    }

    private static bool TryExecuteSynologyApiCommand(SshClient sourceClient, ConnectionInfoData source, string commandText, out string output)
    {
        output = string.Empty;
        string error;
        if (TryExecuteCommandForcedSudo(sourceClient, commandText, source, out output, out error))
        {
            return true;
        }

        if (TryExecuteCommand(sourceClient, commandText, source, out output, out error))
        {
            return true;
        }

        return false;
    }

    private static List<string> ExecuteSynologyVmApiListOutputs(SshClient sourceClient, ConnectionInfoData source)
    {
        var outputs = new List<string>();
        var commands = new[]
        {
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=1",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=2",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=1",
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=2",
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.Guest method=list version=1",
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=list version=1"
        };

        foreach (var command in commands)
        {
            string output;
            if (TryExecuteSynologyApiCommand(sourceClient, source, command, out output) && !string.IsNullOrWhiteSpace(output))
            {
                outputs.Add(output);
            }
        }

        return outputs;
    }

    private static bool TryGetVirtualMachineDefinitionFromSynologyApi(
        SshClient sourceClient,
        ConnectionInfoData source,
        string uuid,
        out VirtualMachineDefinition definition)
    {
        definition = null;
        if (string.IsNullOrWhiteSpace(uuid))
        {
            return false;
        }

        var escapedUuid = ShellQuote(uuid);
        var detailCommands = new[]
        {
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=get version=1 uuid=" + escapedUuid,
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=get version=1 guest_uuid=" + escapedUuid,
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=get version=1 uuid=" + escapedUuid,
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=get version=1 guest_uuid=" + escapedUuid,
            "/usr/syno/sbin/synowebapi --exec api=SYNO.Virtualization.Guest method=get version=2 uuid=" + escapedUuid,
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.Guest method=get version=1 uuid=" + escapedUuid,
            "/usr/syno/bin/synowebapi --exec api=SYNO.Virtualization.API.Guest method=get version=1 uuid=" + escapedUuid
        };

        foreach (var command in detailCommands)
        {
            string output;
            if (!TryExecuteSynologyApiCommand(sourceClient, source, command, out output) || string.IsNullOrWhiteSpace(output))
            {
                continue;
            }

            var parsed = ParseVirtualMachineDefinitionsFromApiOutput(output)
                .FirstOrDefault(item => item != null && string.Equals(item.Uuid, uuid, StringComparison.OrdinalIgnoreCase));
            if (parsed == null)
            {
                parsed = BuildFallbackVirtualMachineDefinitionFromApiText(output, uuid);
                if (parsed == null)
                {
                    continue;
                }
            }

            if (string.IsNullOrWhiteSpace(parsed.Name))
            {
                parsed.Name = uuid;
            }

            if (string.IsNullOrWhiteSpace(parsed.DisplayName))
            {
                parsed.DisplayName = parsed.Name;
            }

            MergeVirtualMachineDisks(parsed.Disks, ExtractVirtualMachineDisksFromApiText(output));

            definition = parsed;
            return true;
        }

        return false;
    }

    private static VirtualMachineDefinition BuildFallbackVirtualMachineDefinitionFromApiText(string output, string uuid)
    {
        if (string.IsNullOrWhiteSpace(output) || string.IsNullOrWhiteSpace(uuid))
        {
            return null;
        }

        var state = Regex.Match(output, "\"(?:state|status|power_status|power_state)\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.IgnoreCase).Groups[1].Value;
        var name = Regex.Match(output, "\"(?:name|vm_name|guest_name|display_name|instance_name|title|description|label)\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.IgnoreCase).Groups[1].Value;
        name = NormalizeOptionalValue(name) ?? uuid;
        var disks = ExtractVirtualMachineDisksFromApiText(output);
        if (disks.Count == 0 && string.IsNullOrWhiteSpace(state))
        {
            return null;
        }

        return new VirtualMachineDefinition
        {
            Name = uuid,
            DisplayName = name,
            Uuid = uuid,
            State = string.IsNullOrWhiteSpace(state) ? "unknown" : state.Trim(),
            Running = !string.IsNullOrWhiteSpace(state) && state.IndexOf("running", StringComparison.OrdinalIgnoreCase) >= 0,
            Autostart = false,
            Vcpus = 0,
            MemoryKiB = 0,
            Disks = disks,
            Interfaces = new List<VirtualMachineInterfaceDefinition>()
        };
    }

    private static List<VirtualMachineDiskDefinition> ExtractVirtualMachineDisksFromApiText(string output)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        var index = 0;

        foreach (Match match in Regex.Matches(output ?? string.Empty, "(/[^\\s\\\"',]+\\.(?:qcow2|img|raw|vmdk|vhd|vhdx|vdi))", RegexOptions.IgnoreCase))
        {
            var path = NormalizeOptionalValue(match.Groups[1].Value);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "disk" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            });
            index++;
        }

        foreach (Match match in Regex.Matches(output ?? string.Empty, "(\"(?:path|file|source|source_path|disk_path|image_path|location|image|disk_file)\"\\s*:\\s*\")([^\"]+)", RegexOptions.IgnoreCase))
        {
            var path = NormalizeOptionalValue(match.Groups[2].Value);
            if (string.IsNullOrWhiteSpace(path) ||
                !path.StartsWith("/", StringComparison.Ordinal) ||
                IsVirtualMachineAuxiliaryDiskPath(path) ||
                !IsLikelyVirtualMachineDiskCandidatePath(path))
            {
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "disk" + index.ToString(CultureInfo.InvariantCulture),
                SourcePath = path
            });
            index++;
        }

        foreach (Match match in Regex.Matches(output ?? string.Empty, "\"pool\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*\"(?:volume|volume_name|name)\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.IgnoreCase))
        {
            var pool = NormalizeOptionalValue(match.Groups[1].Value);
            var volume = NormalizeOptionalValue(match.Groups[2].Value);
            if (string.IsNullOrWhiteSpace(pool) || string.IsNullOrWhiteSpace(volume))
            {
                continue;
            }

            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "disk" + index.ToString(CultureInfo.InvariantCulture),
                SourcePool = pool,
                SourceVolume = volume
            });
            index++;
        }

        return result
            .GroupBy(item => (item.SourcePath ?? string.Empty) + "|" + (item.SourcePool ?? string.Empty) + "|" + (item.SourceVolume ?? string.Empty), StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();
    }

    private static List<VirtualMachineDefinition> ParseVirtualMachineDefinitionsFromApiOutput(string output)
    {
        var definitions = new List<VirtualMachineDefinition>();
        object root = null;
        try
        {
            root = Json.DeserializeObject(output ?? string.Empty);
        }
        catch
        {
            return ParseVirtualMachineDefinitionsFromApiRawText(output);
        }

        foreach (var node in EnumerateSynologyApiObjectDictionaries(root))
        {
            VirtualMachineDefinition definition;
            if (!TryBuildVirtualMachineDefinitionFromApiNode(node, out definition))
            {
                continue;
            }

            definitions.Add(definition);
        }

        if (definitions.Count == 0)
        {
            definitions.AddRange(ParseVirtualMachineDefinitionsFromApiRawText(output));
        }

        return definitions;
    }

    private static List<VirtualMachineDefinition> ParseVirtualMachineDefinitionsFromApiRawText(string output)
    {
        var result = new List<VirtualMachineDefinition>();
        var text = output ?? string.Empty;
        var uuidMatches = Regex.Matches(text, "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
            .Cast<Match>()
            .Select(match => new
            {
                Uuid = NormalizeOptionalValue(match.Value),
                Index = match.Index
            })
            .Where(item => !string.IsNullOrWhiteSpace(item.Uuid))
            .GroupBy(item => item.Uuid, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        foreach (var entry in uuidMatches)
        {
            var start = Math.Max(0, entry.Index - 800);
            var length = Math.Min(text.Length - start, 2600);
            var window = text.Substring(start, length);
            var name = Regex.Match(window, "\"(?:name|vm_name|guest_name|display_name|instance_name|title|description|label|vm_label)\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.IgnoreCase).Groups[1].Value;
            name = NormalizeOptionalValue(name) ?? entry.Uuid;
            var state = Regex.Match(window, "\"(?:state|status|power_status|power_state|guest_status)\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.IgnoreCase).Groups[1].Value;

            var disks = ExtractVirtualMachineDisksFromApiText(window);
            if (disks.Count == 0 && string.Equals(name, entry.Uuid, StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(state))
            {
                continue;
            }

            result.Add(new VirtualMachineDefinition
            {
                Name = entry.Uuid,
                DisplayName = name,
                Uuid = entry.Uuid,
                State = string.IsNullOrWhiteSpace(state) ? "unknown" : state.Trim(),
                Running = !string.IsNullOrWhiteSpace(state) && state.IndexOf("running", StringComparison.OrdinalIgnoreCase) >= 0,
                Autostart = false,
                Vcpus = 0,
                MemoryKiB = 0,
                Disks = disks,
                Interfaces = new List<VirtualMachineInterfaceDefinition>()
            });
        }

        return result;
    }

    private static bool TryBuildVirtualMachineDefinitionFromApiNode(Dictionary<string, object> node, out VirtualMachineDefinition definition)
    {
        definition = null;
        if (node == null || node.Count == 0)
        {
            return false;
        }

        var uuid = GetApiUuid(node);
        var name = GetApiString(node, "name", "vm_name", "guest_name", "display_name", "instance_name", "title", "description", "label", "vm_label");
        if (string.IsNullOrWhiteSpace(uuid))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            name = uuid;
        }

        if (GetApiBool(node, "is_template", "template"))
        {
            return false;
        }

        var disks = ParseVirtualMachineDisksFromApiNode(node);
        var nics = ParseVirtualMachineInterfacesFromApiNode(node);
        var vcpus = Math.Max(0, GetApiInt(node, "vcpu", "vcpus", "cpu", "cpu_count"));
        var memoryKiB = GetApiMemoryKiB(node);
        var stateText = NormalizeOptionalValue(GetApiString(node, "state", "status", "power_status", "power_state", "guest_status")) ?? "unknown";
        var hasSignal =
            disks.Count > 0 ||
            nics.Count > 0 ||
            vcpus > 0 ||
            memoryKiB > 0 ||
            (!string.IsNullOrWhiteSpace(stateText) && !string.Equals(stateText, "unknown", StringComparison.OrdinalIgnoreCase)) ||
            !string.Equals(name, uuid, StringComparison.OrdinalIgnoreCase);
        if (!hasSignal)
        {
            return false;
        }

        definition = new VirtualMachineDefinition
        {
            Name = uuid,
            DisplayName = name,
            Uuid = uuid,
            State = stateText,
            Running = stateText.IndexOf("running", StringComparison.OrdinalIgnoreCase) >= 0,
            Autostart = GetApiBool(node, "autostart", "auto_start", "onboot"),
            Vcpus = vcpus,
            MemoryKiB = memoryKiB,
            Disks = disks,
            Interfaces = new List<VirtualMachineInterfaceDefinition>()
        };

        if (nics.Count > 0)
        {
            definition.Interfaces = nics;
        }

        return true;
    }

    private static List<Dictionary<string, object>> EnumerateSynologyApiObjectDictionaries(object node)
    {
        var result = new List<Dictionary<string, object>>();
        if (node == null)
        {
            return result;
        }

        var dictionary = node as Dictionary<string, object>;
        if (dictionary != null)
        {
            result.Add(dictionary);
            foreach (var value in dictionary.Values)
            {
                result.AddRange(EnumerateSynologyApiObjectDictionaries(value));
            }

            return result;
        }

        var array = node as object[];
        if (array != null)
        {
            foreach (var item in array)
            {
                result.AddRange(EnumerateSynologyApiObjectDictionaries(item));
            }
        }

        return result;
    }

    private static List<VirtualMachineDiskDefinition> ParseVirtualMachineDisksFromApiNode(Dictionary<string, object> node)
    {
        var result = new List<VirtualMachineDiskDefinition>();
        if (node == null || node.Count == 0)
        {
            return result;
        }

        var diskNodes = EnumerateSynologyApiObjectDictionaries(node)
            .Where(LooksLikeApiDiskNode)
            .ToList();

        var index = 0;
        foreach (var diskNode in diskNodes)
        {
            var sourcePath = GetApiString(diskNode, "path", "file", "source", "source_path", "disk_path", "image_path", "location", "image", "disk_file");
            var sourcePool = GetApiString(diskNode, "pool", "storage_pool");
            var sourceVolume = GetApiString(diskNode, "volume", "vol", "volume_name");
            var virtualSizeBytes = GetApiSizeBytes(diskNode,
                "virtual_size", "size", "capacity", "provisioned_size",
                "virtual_size_bytes", "size_bytes", "capacity_bytes", "provisioned_size_bytes",
                "disk_size", "disk_capacity", "total_size", "total_size_bytes");
            var actualSizeBytes = GetApiSizeBytes(diskNode,
                "actual_size", "allocated", "used_size",
                "actual_size_bytes", "allocated_size", "allocated_bytes", "used_bytes", "usage");
            if (string.IsNullOrWhiteSpace(sourcePath) &&
                (string.IsNullOrWhiteSpace(sourcePool) || string.IsNullOrWhiteSpace(sourceVolume)) &&
                virtualSizeBytes <= 0 &&
                actualSizeBytes <= 0)
            {
                continue;
            }

            if (IsVirtualMachineAuxiliaryDiskPath(sourcePath) &&
                string.IsNullOrWhiteSpace(sourcePool) &&
                string.IsNullOrWhiteSpace(sourceVolume))
            {
                continue;
            }

            var disk = new VirtualMachineDiskDefinition
            {
                DeviceType = GetApiString(diskNode, "device_type", "type") ?? "disk",
                TargetName = GetApiString(diskNode, "target", "dev", "device", "disk_id", "name") ?? ("disk" + index.ToString(CultureInfo.InvariantCulture)),
                SourcePath = sourcePath,
                SourcePool = sourcePool,
                SourceVolume = sourceVolume,
                Format = GetApiString(diskNode, "format", "file_format", "image_format"),
                VirtualSizeBytes = virtualSizeBytes,
                ActualSizeBytes = actualSizeBytes
            };

            if (disk.ActualSizeBytes <= 0 && disk.VirtualSizeBytes > 0)
            {
                disk.ActualSizeBytes = disk.VirtualSizeBytes;
            }

            result.Add(disk);
            index++;
        }

        var nodeLevelSizeBytes = GetApiSizeBytes(node,
            "disk_size", "disk_capacity", "disk_total_size",
            "disk_size_bytes", "disk_capacity_bytes", "disk_total_size_bytes",
            "system_disk_size", "system_disk_capacity", "system_disk_size_bytes",
            "volume_size", "volume_capacity", "volume_size_bytes", "volume_capacity_bytes",
            "image_size", "image_capacity", "image_size_bytes", "image_capacity_bytes",
            "storage_size", "storage_capacity", "storage_size_bytes", "storage_capacity_bytes");
        if (nodeLevelSizeBytes <= 0)
        {
            var sizeMb = GetApiLong(node,
                "disk_size_mb", "disk_capacity_mb", "disk_total_size_mb",
                "volume_size_mb", "volume_capacity_mb",
                "image_size_mb", "image_capacity_mb",
                "storage_size_mb", "storage_capacity_mb");
            if (sizeMb > 0)
            {
                nodeLevelSizeBytes = sizeMb * 1024L * 1024L;
            }
        }

        if (nodeLevelSizeBytes <= 0)
        {
            var sizeGb = GetApiLong(node,
                "disk_size_gb", "disk_capacity_gb", "disk_total_size_gb",
                "volume_size_gb", "volume_capacity_gb",
                "image_size_gb", "image_capacity_gb",
                "storage_size_gb", "storage_capacity_gb");
            if (sizeGb > 0)
            {
                nodeLevelSizeBytes = sizeGb * 1024L * 1024L * 1024L;
            }
        }

        var nodeLevelPath = GetApiString(node, "disk_path", "image_path", "path", "file", "source");
        if (IsVirtualMachineAuxiliaryDiskPath(nodeLevelPath))
        {
            nodeLevelPath = null;
        }

        if (result.Count == 0 && (nodeLevelSizeBytes > 0 || !string.IsNullOrWhiteSpace(nodeLevelPath)))
        {
            result.Add(new VirtualMachineDiskDefinition
            {
                DeviceType = "disk",
                TargetName = "disk0",
                SourcePath = nodeLevelPath,
                VirtualSizeBytes = nodeLevelSizeBytes,
                ActualSizeBytes = nodeLevelSizeBytes
            });
        }
        else if (result.Count == 1 &&
                 nodeLevelSizeBytes > 0 &&
                 result[0].VirtualSizeBytes <= 0 &&
                 result[0].ActualSizeBytes <= 0)
        {
            result[0].VirtualSizeBytes = nodeLevelSizeBytes;
            result[0].ActualSizeBytes = nodeLevelSizeBytes;
        }

        return result
            .GroupBy(item => (item.TargetName ?? string.Empty) + "|" + (item.SourcePath ?? string.Empty), StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();
    }

    private static List<VirtualMachineInterfaceDefinition> ParseVirtualMachineInterfacesFromApiNode(Dictionary<string, object> node)
    {
        var result = new List<VirtualMachineInterfaceDefinition>();
        if (node == null || node.Count == 0)
        {
            return result;
        }

        foreach (var nicNode in EnumerateSynologyApiObjectDictionaries(node).Where(LooksLikeApiNicNode))
        {
            var nic = new VirtualMachineInterfaceDefinition
            {
                InterfaceName = GetApiString(nicNode, "name", "id", "nic") ?? ("nic" + result.Count.ToString(CultureInfo.InvariantCulture)),
                Type = GetApiString(nicNode, "type", "nic_type") ?? "network",
                SourceName = GetApiString(nicNode, "bridge", "network", "source"),
                Model = GetApiString(nicNode, "model"),
                MacAddress = GetApiString(nicNode, "mac", "mac_address")
            };

            result.Add(nic);
        }

        return result
            .GroupBy(item => (item.InterfaceName ?? string.Empty) + "|" + (item.MacAddress ?? string.Empty), StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();
    }

    private static bool LooksLikeApiDiskNode(Dictionary<string, object> node)
    {
        var keys = node.Keys.Select(item => (item ?? string.Empty).ToLowerInvariant()).ToList();
        if (keys.Count == 0)
        {
            return false;
        }

        var hasDiskHint = keys.Any(key => key.Contains("disk") || key.Contains("volume") || key == "path" || key == "file" || key == "source");
        if (!hasDiskHint)
        {
            return false;
        }

        var hasSourcePath = keys.Any(key => key == "path" || key == "file" || key == "source" || key == "source_path" || key == "disk_path" || key == "image_path");
        var hasPoolVolume = (keys.Contains("pool") || keys.Contains("storage_pool")) && (keys.Contains("volume") || keys.Contains("volume_name") || keys.Contains("vol"));
        return hasSourcePath || hasPoolVolume;
    }

    private static bool LooksLikeApiNicNode(Dictionary<string, object> node)
    {
        var keys = node.Keys.Select(item => (item ?? string.Empty).ToLowerInvariant()).ToList();
        if (keys.Count == 0)
        {
            return false;
        }

        return keys.Any(key => key.Contains("mac")) &&
               (keys.Any(key => key.Contains("nic")) || keys.Any(key => key.Contains("bridge")) || keys.Any(key => key.Contains("network")));
    }

    private static string GetApiUuid(Dictionary<string, object> node)
    {
        var value = GetApiString(node, "uuid", "guest_uuid", "vm_uuid");
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var normalized = NormalizeOptionalValue(value);
        return Regex.IsMatch(normalized ?? string.Empty, "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
            ? normalized
            : null;
    }

    private static string GetApiString(Dictionary<string, object> node, params string[] keys)
    {
        if (node == null || keys == null)
        {
            return null;
        }

        foreach (var key in keys)
        {
            object value;
            if (node.TryGetValue(key, out value) && value != null)
            {
                var normalized = NormalizeOptionalValue(value.ToString());
                if (!string.IsNullOrWhiteSpace(normalized))
                {
                    return normalized;
                }
            }

            var found = node.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase));
            if (found.Key != null && found.Value != null)
            {
                var normalized = NormalizeOptionalValue(found.Value.ToString());
                if (!string.IsNullOrWhiteSpace(normalized))
                {
                    return normalized;
                }
            }
        }

        return null;
    }

    private static bool GetApiBool(Dictionary<string, object> node, params string[] keys)
    {
        foreach (var key in keys ?? new string[0])
        {
            object value;
            if (node != null && node.TryGetValue(key, out value) && value != null)
            {
                var text = value.ToString().Trim().ToLowerInvariant();
                if (text == "1" || text == "true" || text == "yes" || text == "enabled")
                {
                    return true;
                }
            }

            if (node != null)
            {
                var found = node.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase));
                if (found.Key != null && found.Value != null)
                {
                    var text = found.Value.ToString().Trim().ToLowerInvariant();
                    if (text == "1" || text == "true" || text == "yes" || text == "enabled")
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static int GetApiInt(Dictionary<string, object> node, params string[] keys)
    {
        foreach (var key in keys ?? new string[0])
        {
            object value;
            if (node != null && node.TryGetValue(key, out value) && value != null)
            {
                var parsed = ParseIntValue(value.ToString());
                if (parsed > 0)
                {
                    return parsed;
                }
            }

            if (node != null)
            {
                var found = node.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase));
                if (found.Key != null && found.Value != null)
                {
                    var parsed = ParseIntValue(found.Value.ToString());
                    if (parsed > 0)
                    {
                        return parsed;
                    }
                }
            }
        }

        return 0;
    }

    private static long GetApiLong(Dictionary<string, object> node, params string[] keys)
    {
        foreach (var key in keys ?? new string[0])
        {
            object value;
            if (node != null && node.TryGetValue(key, out value) && value != null)
            {
                var parsed = ParseLongValue(value.ToString());
                if (parsed > 0)
                {
                    return parsed;
                }
            }

            if (node != null)
            {
                var found = node.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase));
                if (found.Key != null && found.Value != null)
                {
                    var parsed = ParseLongValue(found.Value.ToString());
                    if (parsed > 0)
                    {
                        return parsed;
                    }
                }
            }
        }

        return 0;
    }

    private static long GetApiSizeBytes(Dictionary<string, object> node, params string[] keys)
    {
        foreach (var key in keys ?? new string[0])
        {
            object value;
            if (node != null && node.TryGetValue(key, out value) && value != null)
            {
                var parsed = ParseSizeBytesFlexible(value.ToString(), key);
                if (parsed > 0)
                {
                    return parsed;
                }
            }

            if (node != null)
            {
                var found = node.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase));
                if (found.Key != null && found.Value != null)
                {
                    var parsed = ParseSizeBytesFlexible(found.Value.ToString(), found.Key ?? key);
                    if (parsed > 0)
                    {
                        return parsed;
                    }
                }
            }
        }

        return 0;
    }

    private static long ParseSizeBytesFlexible(string value, string keyHint = null)
    {
        var text = NormalizeOptionalValue(value);
        if (string.IsNullOrWhiteSpace(text))
        {
            return 0;
        }

        var bytesMatch = Regex.Match(text, "(\\d+)\\s*bytes", RegexOptions.IgnoreCase);
        if (bytesMatch.Success)
        {
            long bytesParsed;
            if (long.TryParse(bytesMatch.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out bytesParsed) && bytesParsed > 0)
            {
                return bytesParsed;
            }
        }

        var unitMatch = Regex.Match(text, "([0-9]+(?:\\.[0-9]+)?)\\s*([kmgtp]?i?b)", RegexOptions.IgnoreCase);
        if (unitMatch.Success)
        {
            double number;
            if (!double.TryParse(unitMatch.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out number))
            {
                return 0;
            }

            var unit = unitMatch.Groups[2].Value.Trim().ToLowerInvariant();
            var multiplier = 1D;
            switch (unit)
            {
                case "kb":
                case "kib":
                    multiplier = 1024D;
                    break;
                case "mb":
                case "mib":
                    multiplier = 1024D * 1024D;
                    break;
                case "gb":
                case "gib":
                    multiplier = 1024D * 1024D * 1024D;
                    break;
                case "tb":
                case "tib":
                    multiplier = 1024D * 1024D * 1024D * 1024D;
                    break;
                case "pb":
                case "pib":
                    multiplier = 1024D * 1024D * 1024D * 1024D * 1024D;
                    break;
            }

            var bytes = number * multiplier;
            return bytes > 0 ? (long)Math.Round(bytes, MidpointRounding.AwayFromZero) : 0;
        }

        var shortUnitMatch = Regex.Match(text, "([0-9]+(?:\\.[0-9]+)?)\\s*([kmgtp])\\b", RegexOptions.IgnoreCase);
        if (shortUnitMatch.Success)
        {
            return ParseSizeBytesFlexible(shortUnitMatch.Groups[1].Value + " " + shortUnitMatch.Groups[2].Value + "b");
        }

        var pureNumberMatch = Regex.Match(text, "^\\s*([0-9]+(?:\\.[0-9]+)?)\\s*$");
        if (pureNumberMatch.Success)
        {
            double numeric;
            if (double.TryParse(pureNumberMatch.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out numeric) &&
                numeric > 0)
            {
                var hint = (keyHint ?? string.Empty).Trim().ToLowerInvariant();
                var multiplier = 1D;
                if (hint.Contains("kb") || hint.Contains("kib"))
                {
                    multiplier = 1024D;
                }
                else if (hint.Contains("mb") || hint.Contains("mib"))
                {
                    multiplier = 1024D * 1024D;
                }
                else if (hint.Contains("gb") || hint.Contains("gib"))
                {
                    multiplier = 1024D * 1024D * 1024D;
                }
                else if (hint.Contains("tb") || hint.Contains("tib"))
                {
                    multiplier = 1024D * 1024D * 1024D * 1024D;
                }
                else if ((hint.Contains("size") || hint.Contains("capacity") || hint.Contains("alloc") || hint.Contains("used")) &&
                         numeric <= 16384D)
                {
                    multiplier = 1024D * 1024D * 1024D;
                }

                var bytes = numeric * multiplier;
                if (bytes > 0)
                {
                    return (long)Math.Round(bytes, MidpointRounding.AwayFromZero);
                }
            }
        }

        var numericOnly = ParseLongValue(text);
        if (numericOnly > 0)
        {
            return numericOnly;
        }

        return 0;
    }

    private static long GetApiMemoryKiB(Dictionary<string, object> node)
    {
        if (node == null)
        {
            return 0;
        }

        foreach (var key in new[] { "memory_kib", "memory_kb", "mem_kib" })
        {
            var value = GetApiLong(node, key);
            if (value > 0)
            {
                return value;
            }
        }

        foreach (var key in new[] { "memory_mb", "memory_mib", "mem_mb", "ram_mb" })
        {
            var value = GetApiLong(node, key);
            if (value > 0)
            {
                return value * 1024L;
            }
        }

        foreach (var key in new[] { "memory_gb", "memory_gib", "ram_gb" })
        {
            var value = GetApiLong(node, key);
            if (value > 0)
            {
                return value * 1024L * 1024L;
            }
        }

        var raw = GetApiLong(node, "memory", "ram");
        if (raw <= 0)
        {
            return 0;
        }

        if (raw > 1024L * 1024L * 1024L)
        {
            return raw / 1024L;
        }

        if (raw > 1024L * 64L)
        {
            return raw;
        }

        return raw * 1024L;
    }

    private static void AddSynologyVirtualMachineNamesFromApiCommand(
        SshClient sourceClient,
        ConnectionInfoData source,
        string commandText,
        HashSet<string> target,
        bool forceSudo)
    {
        string output;
        string error;
        var succeeded = forceSudo
            ? TryExecuteCommandForcedSudo(sourceClient, commandText, source, out output, out error)
            : TryExecuteCommand(sourceClient, commandText, source, out output, out error);

        if (!succeeded || string.IsNullOrWhiteSpace(output))
        {
            return;
        }

        foreach (var identifier in ParseSynologyVirtualMachineNamesFromApiOutput(output))
        {
            target.Add(identifier);
        }
    }

    private static List<string> ParseSynologyVirtualMachineNamesFromApiOutput(string output)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var text = output ?? string.Empty;

        foreach (Match uuidMatch in Regex.Matches(text, "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
        {
            var value = NormalizeOptionalValue(uuidMatch.Value);
            if (!string.IsNullOrWhiteSpace(value))
            {
                result.Add(value);
            }
        }

        object root = null;
        try
        {
            root = Json.DeserializeObject(text);
        }
        catch
        {
        }

        foreach (var candidate in EnumerateSynologyApiVirtualMachineCandidates(root))
        {
            var normalized = NormalizeOptionalValue(candidate);
            if (string.IsNullOrWhiteSpace(normalized))
            {
                continue;
            }

            if (Regex.IsMatch(normalized, "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"))
            {
                result.Add(normalized);
            }
        }

        return result
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static IEnumerable<string> EnumerateSynologyApiVirtualMachineCandidates(object node)
    {
        var result = new List<string>();
        if (node == null)
        {
            return result;
        }

        var dictionary = node as Dictionary<string, object>;
        if (dictionary != null)
        {
            foreach (var key in new[]
            {
                "uuid",
                "guest_uuid",
                "vm_uuid",
                "id",
                "name",
                "vm_name",
                "guest_name",
                "display_name",
                "instance_name"
            })
            {
                object value;
                if (dictionary.TryGetValue(key, out value) && value != null)
                {
                    result.Add(value.ToString());
                }
            }

            foreach (var entry in dictionary)
            {
                result.AddRange(EnumerateSynologyApiVirtualMachineCandidates(entry.Value));
            }

            return result;
        }

        var array = node as object[];
        if (array != null)
        {
            foreach (var item in array)
            {
                result.AddRange(EnumerateSynologyApiVirtualMachineCandidates(item));
            }
        }

        return result;
    }

    private static void AddSynologyVirtualMachineNamesFromCommand(SshClient sourceClient, ConnectionInfoData source, string commandText, HashSet<string> target, bool forceSudo)
    {
        string output;
        string error;
        var succeeded = forceSudo
            ? TryExecuteCommandForcedSudo(sourceClient, commandText, source, out output, out error)
            : TryExecuteCommand(sourceClient, commandText, source, out output, out error);

        if (!succeeded)
        {
            return;
        }

        foreach (var name in ParseSynologyVirtualMachineNames(output))
        {
            target.Add(name);
        }
    }

    private static List<string> ParseSynologyVirtualMachineNames(string output)
    {
        var result = new List<string>();
        foreach (var rawLine in (output ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 ||
                line.StartsWith("Id", StringComparison.OrdinalIgnoreCase) ||
                Regex.IsMatch(line, "^-{3,}$"))
            {
                continue;
            }

            var match = Regex.Match(line, "^(-|\\d+)\\s+(.+?)\\s{2,}.+$");
            if (match.Success)
            {
                var name = NormalizeOptionalValue(match.Groups[2].Value);
                if (!string.IsNullOrWhiteSpace(name))
                {
                    result.Add(name);
                    continue;
                }
            }

            var parts = Regex.Split(line, "\\s+").Where(item => item.Length > 0).ToArray();
            if (parts.Length == 1)
            {
                result.Add(parts[0]);
                continue;
            }

            if (parts.Length >= 2)
            {
                result.Add(parts[1]);
            }
        }

        return result
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<string> DiscoverSynologyVirtualMachineXmlPaths(SshClient sourceClient, ConnectionInfoData source)
    {
        const string findCommand =
            "for dir in " +
            "/etc/libvirt/qemu " +
            "/var/lib/libvirt/qemu " +
            "/run/libvirt/qemu " +
            "/var/packages/Virtualization/target/etc/libvirt/qemu " +
            "/var/packages/Virtualization/target/var/lib/libvirt/qemu " +
            "/var/packages/Virtualization/etc/libvirt/qemu " +
            "/var/packages/Virtualization/var/lib/libvirt/qemu " +
            "/var/packages/VirtualMachineManager/target/etc/libvirt/qemu " +
            "/var/packages/VirtualMachineManager/target/var/lib/libvirt/qemu " +
            "/var/packages/VirtualMachineManager/etc/libvirt/qemu " +
            "/var/packages/VirtualMachineManager/var/lib/libvirt/qemu; do " +
            "if [ -d \"$dir\" ]; then find \"$dir\" -maxdepth 8 -type f \\( -name '*.xml' -o -name '*.cfg' -o -name '*.conf' \\) 2>/dev/null; fi; " +
            "done; " +
            "for pkg in /var/packages/Virtualization /var/packages/VirtualMachineManager; do " +
            "if [ -d \"$pkg\" ]; then find \"$pkg\" -maxdepth 12 -type f -path '*/libvirt/*' \\( -name '*.xml' -o -name '*.cfg' -o -name '*.conf' \\) 2>/dev/null; fi; " +
            "done";

        const string fallbackFindCommand =
            "for root in /var/packages/Virtualization /var/packages/VirtualMachineManager /var/lib/libvirt /etc/libvirt; do " +
            "if [ -d \"$root\" ]; then " +
            "find \"$root\" -maxdepth 14 -type f \\( -name '*.xml' -o -name '*.cfg' -o -name '*.conf' \\) 2>/dev/null; " +
            "fi; " +
            "done";

        string output;
        string error;
        if (!(TryExecuteCommand(sourceClient, findCommand, source, out output, out error) ||
              TryExecuteCommandForcedSudo(sourceClient, findCommand, source, out output, out error)))
        {
            return new List<string>();
        }
        string fallbackOutput;
        string fallbackError;
        if (TryExecuteCommand(sourceClient, fallbackFindCommand, source, out fallbackOutput, out fallbackError) ||
            TryExecuteCommandForcedSudo(sourceClient, fallbackFindCommand, source, out fallbackOutput, out fallbackError))
        {
            output = (output ?? string.Empty) + Environment.NewLine + (fallbackOutput ?? string.Empty);
        }

        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(item => NormalizeOptionalValue(item))
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<VirtualMachineDiskDefinition> DiscoverVirtualMachineDisksFromRuntimeConfigsForVm(
        SshClient sourceClient,
        ConnectionInfoData source,
        params string[] identifiers)
    {
        var lookupTokens = (identifiers ?? new string[0])
            .Select(item => NormalizeOptionalValue(item))
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (lookupTokens.Count == 0)
        {
            return new List<VirtualMachineDiskDefinition>();
        }

        var configPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in lookupTokens)
        {
            var searchCommand =
                "token=" + ShellQuote(token) + "; " +
                "for dir in /run/libvirt/qemu /var/run/libvirt/qemu /var/lib/libvirt/qemu " +
                "/var/packages/Virtualization/target/var/lib/libvirt/qemu /var/packages/VirtualMachineManager/target/var/lib/libvirt/qemu; do " +
                "if [ -d \"$dir\" ]; then " +
                "find \"$dir\" -maxdepth 6 -type f \\( -name \"*$token*.xml\" -o -name \"*$token*.cfg\" -o -name \"*$token*.conf\" \\) 2>/dev/null; " +
                "fi; " +
                "done";

            string output;
            string error;
            if (!(TryExecuteCommand(sourceClient, searchCommand, source, out output, out error) ||
                  TryExecuteCommandForcedSudo(sourceClient, searchCommand, source, out output, out error)))
            {
                continue;
            }

            foreach (var line in (output ?? string.Empty).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                AddProcessConfigPathCandidate(configPaths, line);
            }
        }

        var result = new List<VirtualMachineDiskDefinition>();
        foreach (var path in configPaths)
        {
            MergeVirtualMachineDisks(result, DiscoverVirtualMachineDisksFromRuntimeConfigFile(sourceClient, source, path));
        }

        return result;
    }

    private static List<VirtualMachineDefinition> DiscoverVirtualMachinesFromFilesystem(SshClient sourceClient, ConnectionInfoData source)
    {
        var xmlPaths = DiscoverSynologyVirtualMachineXmlPaths(sourceClient, source);
        Log("Filesystem VM XML files discovered: {0}", xmlPaths.Count);
        var result = new List<VirtualMachineDefinition>();
        foreach (var xmlPath in xmlPaths)
        {
            string output;
            string error;
            var readCommand = "cat " + ShellQuote(xmlPath);
            if (!(TryExecuteCommand(sourceClient, readCommand, source, out output, out error) ||
                  TryExecuteCommandForcedSudo(sourceClient, readCommand, source, out output, out error)))
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(output) || output.IndexOf("<domain", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var definition = ParseVirtualMachineDefinitionFromXml(output, Path.GetFileNameWithoutExtension(xmlPath));
            if (definition == null || string.IsNullOrWhiteSpace(definition.Name))
            {
                continue;
            }

            foreach (var disk in definition.Disks)
            {
                PopulateVirtualMachineDiskInfo(sourceClient, source, disk, definition.Name);
            }

            result.Add(definition);
        }

        return result
            .GroupBy(BuildVirtualMachineIdentityKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(item => NormalizeOptionalValue(item.DisplayName) ?? item.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static void MergeVirtualMachineDefinitions(List<VirtualMachineDefinition> runtimeDefinitions, List<VirtualMachineDefinition> filesystemDefinitions)
    {
        if (runtimeDefinitions == null)
        {
            return;
        }

        foreach (var candidate in filesystemDefinitions ?? new List<VirtualMachineDefinition>())
        {
            if (candidate == null)
            {
                continue;
            }

            var candidateKey = BuildVirtualMachineIdentityKey(candidate);
            var candidateAliasKey = BuildVirtualMachineAliasKey(candidate);
            var existing = runtimeDefinitions.FirstOrDefault(item => VirtualMachineDefinitionsMatch(item, candidate, candidateKey, candidateAliasKey));

            if (existing == null)
            {
                runtimeDefinitions.Add(candidate);
                continue;
            }

            MergeVirtualMachineDefinitionValues(existing, candidate);
        }

        DeduplicateVirtualMachineDefinitionsByAlias(runtimeDefinitions);
    }

    private static void DeduplicateVirtualMachineDefinitionsByAlias(List<VirtualMachineDefinition> definitions)
    {
        if (definitions == null || definitions.Count <= 1)
        {
            return;
        }

        var grouped = definitions
            .Where(item => item != null)
            .GroupBy(BuildVirtualMachineConsolidationKey, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var consolidated = new List<VirtualMachineDefinition>();
        foreach (var group in grouped)
        {
            var preferred = group
                .OrderByDescending(GetVirtualMachineDefinitionQualityScore)
                .ThenBy(item => NormalizeOptionalValue(item.Uuid) == null ? 1 : 0)
                .ThenBy(item => NormalizeOptionalValue(item.DisplayName) ?? item.Name, StringComparer.OrdinalIgnoreCase)
                .First();

            foreach (var candidate in group)
            {
                if (!ReferenceEquals(candidate, preferred))
                {
                    MergeVirtualMachineDefinitionValues(preferred, candidate);
                }
            }

            consolidated.Add(preferred);
        }

        definitions.Clear();
        definitions.AddRange(consolidated
            .OrderBy(item => NormalizeOptionalValue(item.DisplayName) ?? item.Name, StringComparer.OrdinalIgnoreCase));
    }

    private static void EnrichVirtualMachineDefinitions(SshClient sourceClient, ConnectionInfoData source, List<VirtualMachineDefinition> definitions)
    {
        if (sourceClient == null || definitions == null || definitions.Count == 0)
        {
            return;
        }

        foreach (var definition in definitions)
        {
            if (definition == null)
            {
                continue;
            }

            if (definition.Disks == null)
            {
                definition.Disks = new List<VirtualMachineDiskDefinition>();
            }

            var needsApiDetails =
                !string.IsNullOrWhiteSpace(definition.Uuid) &&
                (definition.Disks.Count == 0 ||
                 definition.Disks.Any(item => item != null && item.ActualSizeBytes <= 0 && item.VirtualSizeBytes <= 0) ||
                 definition.Vcpus <= 0 ||
                 definition.MemoryKiB <= 0 ||
                 definition.Interfaces == null ||
                 definition.Interfaces.Count == 0);

            if (needsApiDetails)
            {
                VirtualMachineDefinition detailsFromApi;
                if (TryGetVirtualMachineDefinitionFromSynologyApi(sourceClient, source, definition.Uuid, out detailsFromApi) &&
                    detailsFromApi != null)
                {
                    MergeVirtualMachineDefinitionValues(definition, detailsFromApi);
                }
            }

            if (!HasUsableVirtualMachineDisks(definition.Disks))
            {
                var runtimeConfigDisks = DiscoverVirtualMachineDisksFromRuntimeConfigsForVm(
                    sourceClient,
                    source,
                    definition.Name,
                    definition.DisplayName,
                    definition.Uuid);
                MergeVirtualMachineDisks(definition.Disks, runtimeConfigDisks);
            }

            if (!HasUsableVirtualMachineDisks(definition.Disks))
            {
                var filesystemDisks = DiscoverVirtualMachineDisksByFilesystem(
                    sourceClient,
                    source,
                    definition.Uuid,
                    definition.Name,
                    definition.DisplayName);
                MergeVirtualMachineDisks(definition.Disks, filesystemDisks);
            }

            var lookupCandidates = BuildVirtualMachineLookupCandidates(definition);
            foreach (var disk in definition.Disks.Where(item => item != null))
            {
                foreach (var lookupName in lookupCandidates)
                {
                    if ((disk.ActualSizeBytes > 0 || disk.VirtualSizeBytes > 0) &&
                        (!string.IsNullOrWhiteSpace(disk.SourcePath) ||
                         !string.IsNullOrWhiteSpace(disk.SourceVolume) ||
                         !string.IsNullOrWhiteSpace(disk.SourcePool)))
                    {
                        break;
                    }

                    PopulateVirtualMachineDiskInfo(sourceClient, source, disk, lookupName);
                    PopulateVirtualMachineDiskInfoFromDomBlkInfo(sourceClient, source, lookupName, disk);
                }
            }

            if (!HasUsableVirtualMachineDisks(definition.Disks))
            {
                Log("[!] VM enrichment unresolved: name={0}, display={1}, uuid={2}",
                    NormalizeOptionalValue(definition.Name) ?? "-",
                    NormalizeOptionalValue(definition.DisplayName) ?? "-",
                    NormalizeOptionalValue(definition.Uuid) ?? "-");
            }
        }
    }

    private static List<string> BuildVirtualMachineLookupCandidates(VirtualMachineDefinition definition)
    {
        var result = new List<string>();
        if (definition == null)
        {
            return result;
        }

        AddVirtualMachineLookupCandidate(result, definition.Name);
        AddVirtualMachineLookupCandidate(result, definition.DisplayName);

        var strippedDisplayName = Regex.Replace(
                NormalizeOptionalValue(definition.DisplayName) ?? string.Empty,
                "^dsm\\s*instance\\s*:\\s*",
                string.Empty,
                RegexOptions.IgnoreCase)
            .Trim();
        AddVirtualMachineLookupCandidate(result, strippedDisplayName);
        AddVirtualMachineLookupCandidate(result, definition.Uuid);

        return result;
    }

    private static void AddVirtualMachineLookupCandidate(List<string> target, string value)
    {
        var normalized = NormalizeOptionalValue(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return;
        }

        if (target.Any(item => string.Equals(item, normalized, StringComparison.OrdinalIgnoreCase)))
        {
            return;
        }

        target.Add(normalized);
    }

    private static string BuildVirtualMachineIdentityKey(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        return NormalizeOptionalValue(definition.Uuid) ??
               BuildVirtualMachineAliasKey(definition) ??
               string.Empty;
    }

    private static string BuildVirtualMachineAliasKey(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        var displayName = NormalizeVirtualMachineNameForMatch(definition.DisplayName);
        var name = NormalizeVirtualMachineNameForMatch(definition.Name);

        if (!string.IsNullOrWhiteSpace(displayName) && !IsLikelyVirtualMachineIdentifier(displayName))
        {
            return displayName;
        }

        if (!string.IsNullOrWhiteSpace(name) && !IsLikelyVirtualMachineIdentifier(name))
        {
            return name;
        }

        return displayName ?? name ?? string.Empty;
    }

    private static string NormalizeVirtualMachineNameForMatch(string value)
    {
        var normalized = NormalizeOptionalValue(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return null;
        }

        normalized = Regex.Replace(normalized, "^dsm\\s*instance\\s*:\\s*", string.Empty, RegexOptions.IgnoreCase).Trim();
        normalized = Regex.Replace(normalized, "^dsm[\\s_-]*instance[\\s:_-]+", string.Empty, RegexOptions.IgnoreCase).Trim();
        normalized = Regex.Replace(normalized, "[\\s_-]+", " ").Trim();
        return normalized.Length == 0 ? null : normalized.ToLowerInvariant();
    }

    private static string BuildVirtualMachineConsolidationKey(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        var aliasKey = BuildVirtualMachineAliasKey(definition);
        if (!string.IsNullOrWhiteSpace(aliasKey))
        {
            return "alias:" + aliasKey;
        }

        var uuidKey = NormalizeOptionalValue(definition.Uuid);
        if (!string.IsNullOrWhiteSpace(uuidKey))
        {
            return "uuid:" + uuidKey.ToLowerInvariant();
        }

        var nameKey = NormalizeOptionalValue(definition.Name);
        if (!string.IsNullOrWhiteSpace(nameKey))
        {
            return "name:" + nameKey.ToLowerInvariant();
        }

        return "object:" + definition.GetHashCode().ToString(CultureInfo.InvariantCulture);
    }

    private static bool VirtualMachineDefinitionsMatch(
        VirtualMachineDefinition left,
        VirtualMachineDefinition right,
        string rightIdentityKey,
        string rightAliasKey)
    {
        if (left == null || right == null)
        {
            return false;
        }

        if (string.Equals(BuildVirtualMachineIdentityKey(left), rightIdentityKey, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(left.Uuid) &&
            !string.IsNullOrWhiteSpace(right.Uuid) &&
            string.Equals(left.Uuid, right.Uuid, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(rightAliasKey) &&
            string.Equals(BuildVirtualMachineAliasKey(left), rightAliasKey, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    private static void MergeVirtualMachineDefinitionValues(VirtualMachineDefinition existing, VirtualMachineDefinition candidate)
    {
        if (existing == null || candidate == null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(existing.DisplayName))
        {
            existing.DisplayName = candidate.DisplayName;
        }
        else if (LooksLikeDsInstancePrefix(existing.DisplayName) && !LooksLikeDsInstancePrefix(candidate.DisplayName))
        {
            existing.DisplayName = candidate.DisplayName;
        }

        if (string.IsNullOrWhiteSpace(existing.Uuid))
        {
            existing.Uuid = candidate.Uuid;
        }

        if (existing.MemoryKiB <= 0)
        {
            existing.MemoryKiB = candidate.MemoryKiB;
        }

        if (existing.Vcpus <= 0)
        {
            existing.Vcpus = candidate.Vcpus;
        }

        if (string.IsNullOrWhiteSpace(existing.OsType))
        {
            existing.OsType = candidate.OsType;
        }

        if (string.IsNullOrWhiteSpace(existing.MachineType))
        {
            existing.MachineType = candidate.MachineType;
        }

        if ((existing.Interfaces == null || existing.Interfaces.Count == 0) && candidate.Interfaces != null && candidate.Interfaces.Count > 0)
        {
            existing.Interfaces = candidate.Interfaces;
        }

        if (existing.Disks == null || existing.Disks.Count == 0)
        {
            existing.Disks = candidate.Disks ?? new List<VirtualMachineDiskDefinition>();
        }
        else
        {
            MergeVirtualMachineDisks(existing.Disks, candidate.Disks);
        }
    }

    private static int GetVirtualMachineDefinitionQualityScore(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return 0;
        }

        var score = 0;
        if (!string.IsNullOrWhiteSpace(definition.Uuid))
        {
            score += 50;
        }

        if (definition.Vcpus > 0)
        {
            score += 10;
        }

        if (definition.MemoryKiB > 0)
        {
            score += 10;
        }

        if (definition.Interfaces != null && definition.Interfaces.Count > 0)
        {
            score += 8;
        }

        if (!string.IsNullOrWhiteSpace(definition.State) && !string.Equals(definition.State, "unknown", StringComparison.OrdinalIgnoreCase))
        {
            score += 4;
        }

        foreach (var disk in definition.Disks ?? new List<VirtualMachineDiskDefinition>())
        {
            if (disk == null)
            {
                continue;
            }

            score += 6;
            if (!string.IsNullOrWhiteSpace(disk.SourcePath) ||
                !string.IsNullOrWhiteSpace(disk.SourceVolume) ||
                !string.IsNullOrWhiteSpace(disk.SourcePool))
            {
                score += 12;
            }

            if (disk.ActualSizeBytes > 0 || disk.VirtualSizeBytes > 0)
            {
                score += 10;
            }
        }

        return score;
    }

    private static bool LooksLikeDsInstancePrefix(string value)
    {
        return !string.IsNullOrWhiteSpace(value) &&
               Regex.IsMatch(value, "^dsm\\s*instance\\s*:\\s*", RegexOptions.IgnoreCase);
    }

    private static long GetVirtualMachineDiskVirtualSizeBytes(SshClient sourceClient, ConnectionInfoData source, string sourcePath)
    {
        return TryReadRemoteLong(sourceClient, source,
                   "qemu-img info " + ShellQuote(sourcePath) + " 2>/dev/null | awk -F'[()]' '/virtual size/ {gsub(/[^0-9]/, \"\", $2); print $2; exit}'",
                   true) ??
               TryReadRemoteLong(sourceClient, source,
                   "if command -v blockdev >/dev/null 2>&1; then blockdev --getsize64 " + ShellQuote(sourcePath) + " 2>/dev/null; fi",
                   true) ??
               0L;
    }

    private static long GetVirtualMachineDiskActualSizeBytes(SshClient sourceClient, ConnectionInfoData source, string sourcePath)
    {
        return TryReadRemoteLong(sourceClient, source,
                   "qemu-img info " + ShellQuote(sourcePath) + " 2>/dev/null | awk -F'[()]' '/disk size/ {gsub(/[^0-9]/, \"\", $2); print $2; exit}'",
                   true) ??
               TryReadRemoteLong(sourceClient, source,
                   "stat -Lc %s " + ShellQuote(sourcePath) + " 2>/dev/null",
                   true) ??
               TryReadRemoteLong(sourceClient, source,
                   "if [ -b " + ShellQuote(sourcePath) + " ] && command -v blockdev >/dev/null 2>&1; then blockdev --getsize64 " + ShellQuote(sourcePath) + " 2>/dev/null; fi",
                   true) ??
               GetRemotePathSizeBytes(sourceClient, source, sourcePath);
    }

    private static long? TryReadRemoteLong(SshClient client, ConnectionInfoData connectionInfo, string commandText, bool allowSudo)
    {
        string output;
        string error;
        if (TryExecuteCommand(client, commandText, connectionInfo, out output, out error))
        {
            var parsed = ParseLongValue(output);
            if (parsed > 0)
            {
                return parsed;
            }
        }

        if (allowSudo && TryExecuteCommandForcedSudo(client, commandText, connectionInfo, out output, out error))
        {
            var parsed = ParseLongValue(output);
            if (parsed > 0)
            {
                return parsed;
            }
        }

        return null;
    }

    private static string GetDictionaryValue(Dictionary<string, string> map, string key)
    {
        string value;
        return map != null && map.TryGetValue(key, out value) ? value : null;
    }

    private static int ParseIntValue(string value)
    {
        int parsed;
        return int.TryParse(Regex.Match(value ?? string.Empty, "-?\\d+").Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed)
            ? parsed
            : 0;
    }

    private static long ParseLongValue(string value)
    {
        long parsed;
        return long.TryParse(Regex.Match(value ?? string.Empty, "-?\\d+").Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed)
            ? parsed
            : 0L;
    }

    private static long ParseMemoryKiB(string value)
    {
        var parsed = ParseLongValue(value);
        if (parsed <= 0)
        {
            return 0;
        }

        return value != null && value.IndexOf("MiB", StringComparison.OrdinalIgnoreCase) >= 0
            ? parsed * 1024L
            : parsed;
    }

    private static long ParseMemoryKiBFromDomainXml(string xml)
    {
        var memoryValue = GetRegexGroupValue(xml, "<memory(?:\\s+[^>]*)?>\\s*([^<]+?)\\s*</memory>", 1);
        if (string.IsNullOrWhiteSpace(memoryValue))
        {
            return 0;
        }

        var unit = GetRegexGroupValue(xml, "<memory[^>]*unit=['\"]([^'\"]+)['\"]", 1);
        var parsed = ParseLongValue(memoryValue);
        if (parsed <= 0)
        {
            return 0;
        }

        var normalizedUnit = (unit ?? "kib").Trim().ToLowerInvariant();
        if (normalizedUnit == "kib" || normalizedUnit == "kb")
        {
            return parsed;
        }

        if (normalizedUnit == "mib" || normalizedUnit == "mb")
        {
            return parsed * 1024L;
        }

        if (normalizedUnit == "gib" || normalizedUnit == "gb")
        {
            return parsed * 1024L * 1024L;
        }

        if (normalizedUnit == "b")
        {
            return parsed / 1024L;
        }

        return parsed;
    }

    private static string GetRegexGroupValue(string text, string pattern, int groupIndex)
    {
        var match = Regex.Match(text ?? string.Empty, pattern, RegexOptions.IgnoreCase);
        return match.Success ? NormalizeOptionalValue(match.Groups[groupIndex].Value) : null;
    }

    private static string GetRegexGroupValueSingleline(string text, string pattern, int groupIndex)
    {
        var match = Regex.Match(text ?? string.Empty, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? NormalizeOptionalValue(match.Groups[groupIndex].Value) : null;
    }

    private static string ResolveVirtualMachineDisplayName(string xml, string fallbackName)
    {
        var metadata = GetRegexGroupValueSingleline(xml, "<metadata>(.*?)</metadata>", 1);
        var candidates = new[]
        {
            GetRegexGroupValue(xml, "<title>\\s*([^<]+?)\\s*</title>", 1),
            GetRegexGroupValue(xml, "<description>\\s*([^<]+?)\\s*</description>", 1),
            GetRegexGroupValueSingleline(metadata, "<(?:[A-Za-z0-9_-]+:)?displayname[^>]*>\\s*([^<]+?)\\s*</(?:[A-Za-z0-9_-]+:)?displayname>", 1),
            GetRegexGroupValueSingleline(metadata, "<(?:[A-Za-z0-9_-]+:)?vmname[^>]*>\\s*([^<]+?)\\s*</(?:[A-Za-z0-9_-]+:)?vmname>", 1),
            GetRegexGroupValueSingleline(metadata, "<(?:[A-Za-z0-9_-]+:)?name[^>]*>\\s*([^<]+?)\\s*</(?:[A-Za-z0-9_-]+:)?name>", 1)
        };

        foreach (var candidate in candidates)
        {
            var normalized = NormalizeOptionalValue(candidate);
            if (normalized == null)
            {
                continue;
            }

            if (string.Equals(normalized, fallbackName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (IsLikelyVirtualMachineIdentifier(normalized))
            {
                continue;
            }

            return normalized;
        }

        return fallbackName;
    }

    private static bool IsLikelyVirtualMachineIdentifier(string value)
    {
        var normalized = NormalizeOptionalValue(value);
        if (normalized == null)
        {
            return false;
        }

        return Regex.IsMatch(normalized, "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", RegexOptions.IgnoreCase) ||
               Regex.IsMatch(normalized, "^[0-9a-f]{32}$", RegexOptions.IgnoreCase);
    }

    private static ContainerDefinition ParseContainerDefinition(string inspectJson, string requestedName, string targetRoot)
    {
        var parsed = Json.DeserializeObject(inspectJson) as object[];
        if (parsed == null || parsed.Length == 0)
        {
            throw new InvalidOperationException("docker inspect returned no data for " + requestedName);
        }

        var doc = (Dictionary<string, object>)parsed[0];
        var hostConfig = GetDict(doc, "HostConfig");
        var config = GetDict(doc, "Config");
        var state = GetDict(doc, "State");
        var networkSettings = GetDict(doc, "NetworkSettings");

        var definition = new ContainerDefinition
        {
            Id = GetString(doc, "Id"),
            Name = TrimSlash(GetString(doc, "Name")) ?? requestedName,
            Image = GetString(config, "Image"),
            Hostname = GetString(config, "Hostname"),
            WorkingDir = GetString(config, "WorkingDir"),
            User = GetString(config, "User"),
            RestartPolicyName = GetString(GetDict(hostConfig, "RestartPolicy"), "Name"),
            NetworkMode = GetString(hostConfig, "NetworkMode"),
            Privileged = GetBool(hostConfig, "Privileged"),
            Tty = GetBool(config, "Tty"),
            OpenStdin = GetBool(config, "OpenStdin"),
            Running = GetBool(state, "Running"),
            Status = GetString(state, "Status")
        };

        definition.Command = GetStringArray(config, "Cmd");
        definition.Entrypoint = GetStringArray(config, "Entrypoint");
        definition.Environment = GetStringArray(config, "Env");
        definition.ExtraHosts = GetStringArray(hostConfig, "ExtraHosts");
        definition.CapAdd = GetStringArray(hostConfig, "CapAdd");
        definition.Devices = ParseDevices(hostConfig);
        definition.NetworkAttachments = ParseNetworkAttachments(networkSettings);
        definition.Networks = definition.NetworkAttachments.Select(item => item.Name).ToList();
        definition.PortBindings = ParsePorts(hostConfig, networkSettings);
        definition.ExposedPorts = ParseExposedPorts(config);
        definition.Labels = ParseStringMap(GetDict(config, "Labels"));
        definition.ComposeProject = definition.Labels.ContainsKey("com.docker.compose.project")
            ? definition.Labels["com.docker.compose.project"]
            : string.Empty;
        definition.Mounts = ParseMounts(doc, definition.Name, targetRoot);
        return definition;
    }

    private static DockerNetworkDefinition ParseDockerNetworkDefinition(string inspectJson)
    {
        var parsed = Json.DeserializeObject(inspectJson) as object[];
        if (parsed == null || parsed.Length == 0)
        {
            throw new InvalidOperationException("docker network inspect returned no data.");
        }

        var doc = (Dictionary<string, object>)parsed[0];
        var options = ParseStringMap(GetDict(doc, "Options"));
        var containers = ParseDockerNetworkContainers(GetDict(doc, "Containers"));
        var definition = new DockerNetworkDefinition
        {
            Id = GetString(doc, "Id"),
            Name = GetString(doc, "Name"),
            Driver = GetString(doc, "Driver"),
            Scope = GetString(doc, "Scope"),
            Internal = GetBool(doc, "Internal"),
            Attachable = GetBool(doc, "Attachable"),
            EnableIPv6 = GetBool(doc, "EnableIPv6"),
            Options = options,
            IpamConfigs = ParseDockerNetworkIpamConfigs(GetDict(doc, "IPAM")),
            ConnectedContainers = containers
        };

        definition.ParentInterface = options.ContainsKey("parent") ? options["parent"] : string.Empty;
        definition.IpvlanMode = options.ContainsKey("ipvlan_mode") ? options["ipvlan_mode"] : string.Empty;
        definition.MacvlanMode = options.ContainsKey("macvlan_mode") ? options["macvlan_mode"] : string.Empty;
        return definition;
    }

    private static List<DockerNetworkIpamConfig> ParseDockerNetworkIpamConfigs(Dictionary<string, object> ipam)
    {
        var result = new List<DockerNetworkIpamConfig>();
        foreach (var item in GetObjectArray(ipam, "Config").OfType<Dictionary<string, object>>())
        {
            result.Add(new DockerNetworkIpamConfig
            {
                Subnet = GetString(item, "Subnet"),
                Gateway = GetString(item, "Gateway"),
                IpRange = GetString(item, "IPRange")
            });
        }

        return result;
    }

    private static List<DockerNetworkContainerReference> ParseDockerNetworkContainers(Dictionary<string, object> containers)
    {
        var result = new List<DockerNetworkContainerReference>();
        foreach (var kvp in containers)
        {
            var item = kvp.Value as Dictionary<string, object> ?? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            result.Add(new DockerNetworkContainerReference
            {
                Id = kvp.Key,
                Name = GetString(item, "Name"),
                IPv4Address = GetString(item, "IPv4Address"),
                IPv6Address = GetString(item, "IPv6Address")
            });
        }

        return result.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static List<DeviceMapping> ParseDevices(Dictionary<string, object> hostConfig)
    {
        var items = GetObjectArray(hostConfig, "Devices");
        var result = new List<DeviceMapping>();
        foreach (var item in items.OfType<Dictionary<string, object>>())
        {
            result.Add(new DeviceMapping
            {
                PathOnHost = GetString(item, "PathOnHost"),
                PathInContainer = GetString(item, "PathInContainer"),
                CgroupPermissions = GetString(item, "CgroupPermissions")
            });
        }

        return result;
    }

    private static List<ContainerNetworkAttachment> ParseNetworkAttachments(Dictionary<string, object> networkSettings)
    {
        var networks = GetDict(networkSettings, "Networks");
        var result = new List<ContainerNetworkAttachment>();

        foreach (var kvp in networks.OrderBy(item => item.Key, StringComparer.OrdinalIgnoreCase))
        {
            var endpoint = kvp.Value as Dictionary<string, object> ?? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            var ipamConfig = GetDict(endpoint, "IPAMConfig");
            var explicitIpv4Address = NormalizeOptionalValue(GetString(ipamConfig, "IPv4Address"));
            var explicitIpv6Address = NormalizeOptionalValue(GetString(ipamConfig, "IPv6Address"));
            var ipv4Address = explicitIpv4Address ??
                              NormalizeOptionalValue(GetString(endpoint, "IPAddress"));
            var ipv6Address = explicitIpv6Address ??
                              NormalizeOptionalValue(GetString(endpoint, "GlobalIPv6Address"));

            result.Add(new ContainerNetworkAttachment
            {
                Name = kvp.Key,
                Aliases = GetStringArray(endpoint, "Aliases")
                    .Where(item => !string.IsNullOrWhiteSpace(item))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
                    .ToList(),
                IPv4Address = ipv4Address,
                IPv6Address = ipv6Address,
                HasExplicitIpamConfiguration = explicitIpv4Address != null || explicitIpv6Address != null
            });
        }

        return result;
    }

    private static List<PortBindingItem> ParsePorts(Dictionary<string, object> hostConfig, Dictionary<string, object> networkSettings)
    {
        var result = new List<PortBindingItem>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in GetDict(networkSettings, "Ports"))
        {
            var mappings = kvp.Value as object[];
            if (mappings == null || mappings.Length == 0)
            {
                continue;
            }

            foreach (var raw in mappings.OfType<Dictionary<string, object>>())
            {
                AddPortBinding(result, seen, kvp.Key, GetString(raw, "HostIp"), GetString(raw, "HostPort"));
            }
        }

        var portBindings = GetDict(hostConfig, "PortBindings");
        foreach (var kvp in portBindings)
        {
            var mappings = kvp.Value as object[];
            if (mappings == null || mappings.Length == 0)
            {
                continue;
            }

            foreach (var raw in mappings.OfType<Dictionary<string, object>>())
            {
                AddPortBinding(result, seen, kvp.Key, GetString(raw, "HostIp"), GetString(raw, "HostPort"));
            }
        }

        return result;
    }

    private static void AddPortBinding(List<PortBindingItem> target, HashSet<string> seen, string containerPort, string hostIp, string hostPort)
    {
        if (string.IsNullOrWhiteSpace(containerPort) || string.IsNullOrWhiteSpace(hostPort))
        {
            return;
        }

        var normalizedHostIp = NormalizePortBindingHostIp(hostIp);
        var key = string.Join("|", containerPort.Trim(), normalizedHostIp, hostPort.Trim());
        if (!seen.Add(key))
        {
            return;
        }

        target.Add(new PortBindingItem
        {
            ContainerPort = containerPort,
            HostIp = normalizedHostIp,
            HostPort = hostPort
        });
    }

    private static List<string> ParseExposedPorts(Dictionary<string, object> config)
    {
        return GetDict(config, "ExposedPorts")
            .Keys
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string BuildComposePortMapping(PortBindingItem port)
    {
        var containerPort = (port.ContainerPort ?? string.Empty).Trim();
        var hostPort = (port.HostPort ?? string.Empty).Trim();
        var hostIp = NormalizePortBindingHostIp(port.HostIp);

        if (hostIp.Length == 0)
        {
            return hostPort + ":" + containerPort;
        }

        if (hostIp.IndexOf(':') >= 0 && !hostIp.StartsWith("[", StringComparison.Ordinal))
        {
            hostIp = "[" + hostIp + "]";
        }

        return hostIp + ":" + hostPort + ":" + containerPort;
    }

    private static string NormalizePortBindingHostIp(string hostIp)
    {
        var normalized = NormalizeOptionalValue(hostIp) ?? string.Empty;
        if (string.Equals(normalized, "0.0.0.0", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(normalized, "::", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(normalized, "[::]", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(normalized, "*", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        return normalized;
    }

    private static bool HasDetailedNetworkSettings(ContainerDefinition definition)
    {
        return definition.NetworkAttachments.Any(HasDetailedNetworkSettings);
    }

    private static bool HasDetailedNetworkSettings(ContainerNetworkAttachment attachment)
    {
        return ((attachment.HasExplicitIpamConfiguration && !string.IsNullOrWhiteSpace(attachment.IPv4Address)) ||
                (attachment.HasExplicitIpamConfiguration && !string.IsNullOrWhiteSpace(attachment.IPv6Address))) ||
               (attachment.Aliases != null && attachment.Aliases.Count > 0);
    }

    private static List<string> GetExposedPortsForCompose(ContainerDefinition definition)
    {
        var published = new HashSet<string>(
            definition.PortBindings.Select(item => (item.ContainerPort ?? string.Empty).Trim()),
            StringComparer.OrdinalIgnoreCase);

        return definition.ExposedPorts
            .Where(item => !published.Contains((item ?? string.Empty).Trim()))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<string> GetPortMappingsForCompose(ContainerDefinition definition)
    {
        return definition.PortBindings
            .Select(BuildComposePortMapping)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static IEnumerable<ContainerNetworkAttachment> GetNetworkAttachmentsForCompose(ContainerDefinition definition)
    {
        if (definition.NetworkAttachments.Count > 0)
        {
            return definition.NetworkAttachments.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase);
        }

        return definition.Networks
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .Select(item => new ContainerNetworkAttachment { Name = item })
            .ToList();
    }

    private static void ApplyNetworkOverride(ContainerDefinition definition, List<ContainerNetworkOverride> overrides)
    {
        if (definition == null || overrides == null || overrides.Count == 0)
        {
            return;
        }

        var networkOverride = overrides.FirstOrDefault(item =>
            item != null &&
            string.Equals(item.ContainerName, definition.Name, StringComparison.OrdinalIgnoreCase));

        if (networkOverride == null)
        {
            return;
        }

        var mode = (networkOverride.Mode ?? string.Empty).Trim().ToLowerInvariant();
        if (mode.Length == 0 || string.Equals(mode, NetworkOverrideModes.Source, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var primaryAttachment = GetPrimaryNetworkAttachment(definition);
        if (string.Equals(mode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            definition.NetworkMode = "host";
            definition.Networks = new List<string> { "host" };
            definition.NetworkAttachments = new List<ContainerNetworkAttachment>();
            definition.PortBindings = new List<PortBindingItem>();
            definition.ExposedPorts = new List<string>();
            return;
        }

        if (string.Equals(mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase))
        {
            var networkName = (networkOverride.NetworkName ?? string.Empty).Trim();
            if (networkName.Length == 0)
            {
                throw new InvalidOperationException("Target network is required for container " + definition.Name + ".");
            }

            if (!IsUserDefinedNetwork(networkName))
            {
                throw new InvalidOperationException("Container " + definition.Name + " requires a user-defined Debian Docker network for mode " + mode + ". Built-in networks host/bridge/none are not valid here.");
            }

            var attachment = new ContainerNetworkAttachment
            {
                Name = networkName,
                Aliases = BuildPreservedAliases(definition, primaryAttachment),
                HasExplicitIpamConfiguration = false
            };

            if (string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase))
            {
                attachment.IPv4Address = NormalizeOptionalValue(networkOverride.IPv4Address);
                attachment.IPv6Address = NormalizeOptionalValue(networkOverride.IPv6Address);
                attachment.HasExplicitIpamConfiguration = true;
                if (string.IsNullOrWhiteSpace(attachment.IPv4Address) && string.IsNullOrWhiteSpace(attachment.IPv6Address))
                {
                    throw new InvalidOperationException("Static network mode for container " + definition.Name + " requires IPv4 or IPv6 address.");
                }
            }

            definition.NetworkMode = string.Empty;
            definition.Networks = new List<string> { networkName };
            definition.NetworkAttachments = new List<ContainerNetworkAttachment> { attachment };
        }
    }

    private static ContainerNetworkAttachment GetPrimaryNetworkAttachment(ContainerDefinition definition)
    {
        return definition.NetworkAttachments
            .FirstOrDefault(item => IsUserDefinedNetwork(item.Name)) ??
               definition.NetworkAttachments.FirstOrDefault();
    }

    private static List<string> BuildPreservedAliases(ContainerDefinition definition, ContainerNetworkAttachment primaryAttachment)
    {
        var aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var attachment in definition.NetworkAttachments)
        {
            foreach (var alias in attachment.Aliases ?? new List<string>())
            {
                var normalized = NormalizeOptionalValue(alias);
                if (normalized != null &&
                    !string.Equals(normalized, definition.Name, StringComparison.OrdinalIgnoreCase))
                {
                    aliases.Add(normalized);
                }
            }
        }

        if (primaryAttachment != null)
        {
            foreach (var alias in primaryAttachment.Aliases ?? new List<string>())
            {
                var normalized = NormalizeOptionalValue(alias);
                if (normalized != null &&
                    !string.Equals(normalized, definition.Name, StringComparison.OrdinalIgnoreCase))
                {
                    aliases.Add(normalized);
                }
            }
        }

        return aliases.OrderBy(item => item, StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static string NormalizeOptionalValue(string value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private static string BuildDockerNetworkCreateCommand(DockerNetworkCreateRequest request)
    {
        if (request == null)
        {
            throw new InvalidOperationException("Docker network request is missing.");
        }

        var driver = NormalizeOptionalValue(request.Driver);
        var name = NormalizeOptionalValue(request.Name);
        var parentInterface = NormalizeOptionalValue(request.ParentInterface);
        var subnet = NormalizeOptionalValue(request.Subnet);
        var gateway = NormalizeOptionalValue(request.Gateway);
        var ipRange = NormalizeOptionalValue(request.IpRange);
        var ipvlanMode = NormalizeOptionalValue(request.IpvlanMode) ?? "l2";

        if (!string.Equals(driver, "macvlan", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(driver, "ipvlan", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Only macvlan and ipvlan network creation is supported.");
        }

        if (name == null)
        {
            throw new InvalidOperationException("Docker network name is required.");
        }

        if (!IsUserDefinedNetwork(name))
        {
            throw new InvalidOperationException("Network name host/bridge/none is reserved by Docker.");
        }

        if (parentInterface == null)
        {
            throw new InvalidOperationException("Parent interface is required.");
        }

        if (subnet == null)
        {
            throw new InvalidOperationException("Subnet is required.");
        }

        var parts = new List<string>
        {
            "docker network create",
            "-d " + driver,
            "--subnet=" + ShellQuote(subnet)
        };

        if (gateway != null)
        {
            parts.Add("--gateway=" + ShellQuote(gateway));
        }

        if (ipRange != null)
        {
            parts.Add("--ip-range=" + ShellQuote(ipRange));
        }

        parts.Add("-o parent=" + ShellQuote(parentInterface));

        if (string.Equals(driver, "ipvlan", StringComparison.OrdinalIgnoreCase))
        {
            parts.Add("-o ipvlan_mode=" + ShellQuote(ipvlanMode));
        }

        parts.Add(ShellQuote(name));
        return string.Join(" ", parts);
    }

    private static Dictionary<string, string> ParseStringMap(Dictionary<string, object> data)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in data)
        {
            result[kvp.Key] = kvp.Value == null ? string.Empty : kvp.Value.ToString();
        }

        return result;
    }

    private static List<MountDefinition> ParseMounts(Dictionary<string, object> doc, string containerName, string targetRoot)
    {
        var rawMounts = GetObjectArray(doc, "Mounts");
        var mounts = new List<MountDefinition>();
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var raw in rawMounts.OfType<Dictionary<string, object>>())
        {
            var type = GetString(raw, "Type");
            var source = GetString(raw, "Source");
            var destination = GetString(raw, "Destination");

            if (string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(source) || string.IsNullOrWhiteSpace(destination))
            {
                continue;
            }

            var localName = BuildUniqueName(containerName + "-" + PathToken(destination), usedNames);
            var archiveName = localName + ".tgz";
            var mount = new MountDefinition
            {
                Type = type,
                SourcePath = source,
                DestinationPath = destination,
                ReadOnly = GetBool(raw, "RW") == false,
                ArchiveFileName = archiveName,
                SafeName = localName
            };

            if (string.Equals(type, "bind", StringComparison.OrdinalIgnoreCase))
            {
                mount.TargetSource = targetRoot.TrimEnd('/') + "/binds/" + containerName + "/" + localName;
            }
            else if (string.Equals(type, "volume", StringComparison.OrdinalIgnoreCase))
            {
                mount.VolumeName = GetString(raw, "Name");
                if (string.IsNullOrWhiteSpace(mount.VolumeName))
                {
                    mount.VolumeName = localName;
                }
            }
            else
            {
                continue;
            }

            mounts.Add(mount);
        }

        return mounts;
    }

    private static void BackupContainerData(SshClient sourceClient, ConnectionInfoData sourceInfo, SshClient targetClient, ConnectionInfoData targetInfo, SftpClient targetSftp, ContainerDefinition definition, string stagingRoot)
    {
        foreach (var mount in definition.Mounts)
        {
            Log("Backing up {0}:{1}", definition.Name, mount.DestinationPath);
            var remoteArchiveDir = stagingRoot.TrimEnd('/') + "/archives";
            ExecuteCommand(targetClient, "mkdir -p " + ShellQuote(remoteArchiveDir), targetInfo);

            mount.SourceIsDirectory = RemotePathIsDirectory(sourceClient, sourceInfo, mount.SourcePath);
            var remoteArchivePath = remoteArchiveDir + "/" + mount.ArchiveFileName;
            var tempArchivePath = DownloadTarGzToTempFile(sourceClient, sourceInfo, mount.SourcePath, mount.ArchiveFileName);
            try
            {
                UploadLocalFile(targetSftp, tempArchivePath, remoteArchivePath);
            }
            finally
            {
                DeleteLocalTempFile(tempArchivePath);
            }
        }
    }

    private static void BackupImages(SshClient sourceClient, ConnectionInfoData sourceInfo, SftpClient targetSftp, List<ImageArchive> images, string stagingRoot)
    {
        foreach (var image in images)
        {
            Log("Exporting image {0}", image.Image);
            var tempArchivePath = DownloadCommandOutputToTempFile(sourceClient, sourceInfo, "docker save " + ShellQuote(image.Image) + " | gzip -1", image.ArchiveFileName);
            try
            {
                UploadLocalFile(targetSftp, tempArchivePath, stagingRoot.TrimEnd('/') + "/archives/" + image.ArchiveFileName);
            }
            finally
            {
                DeleteLocalTempFile(tempArchivePath);
            }
        }
    }

    private static void RestoreImages(SshClient targetClient, ConnectionInfoData targetInfo, List<ImageArchive> images, string stagingRoot)
    {
        foreach (var image in images)
        {
            if (ImageExistsOnTarget(targetClient, targetInfo, image.Image))
            {
                Log("Image {0} already exists on Debian, skipping transfer.", image.Image);
                Log("Image ready on Debian {0}", image.Image);
                continue;
            }

            if (TryPullImageOnTarget(targetClient, targetInfo, image.Image))
            {
                Log("Image {0} pulled successfully on Debian, skipping archive load.", image.Image);
                Log("Image ready on Debian {0}", image.Image);
                continue;
            }

            Log("Loading image {0}", image.Image);
            var archivePath = stagingRoot.TrimEnd('/') + "/archives/" + image.ArchiveFileName;
            ExecuteCommand(targetClient, "gzip -dc " + ShellQuote(archivePath) + " | docker load", targetInfo);
            Log("Image ready on Debian {0}", image.Image);
        }
    }

    private static void PopulateContainerSizeEstimates(SshClient sourceClient, ConnectionInfoData sourceInfo, List<ContainerDefinition> definitions)
    {
        if (definitions == null || definitions.Count == 0)
        {
            return;
        }

        var imageSizeCache = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var pathSizeCache = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        foreach (var definition in definitions)
        {
            definition.EstimatedImageBytes = GetCachedImageSizeBytes(sourceClient, sourceInfo, definition.Image, imageSizeCache);
            long dataBytes = 0;
            foreach (var mount in definition.Mounts)
            {
                var sourcePath = NormalizeOptionalValue(mount.SourcePath);
                if (sourcePath == null)
                {
                    continue;
                }

                long sizeBytes;
                if (!pathSizeCache.TryGetValue(sourcePath, out sizeBytes))
                {
                    sizeBytes = GetRemotePathSizeBytes(sourceClient, sourceInfo, sourcePath);
                    pathSizeCache[sourcePath] = sizeBytes;
                }

                dataBytes += sizeBytes;
            }

            definition.EstimatedDataBytes = dataBytes;
        }
    }

    private static long GetCachedImageSizeBytes(SshClient sourceClient, ConnectionInfoData sourceInfo, string imageName, Dictionary<string, long> cache)
    {
        var normalized = NormalizeOptionalValue(imageName);
        if (normalized == null)
        {
            return 0;
        }

        long sizeBytes;
        if (cache.TryGetValue(normalized, out sizeBytes))
        {
            return sizeBytes;
        }

        sizeBytes = GetImageSizeBytes(sourceClient, sourceInfo, normalized);
        cache[normalized] = sizeBytes;
        return sizeBytes;
    }

    private static long GetImageSizeBytes(SshClient sourceClient, ConnectionInfoData sourceInfo, string imageName)
    {
        var output = ExecuteCommand(sourceClient, "docker image inspect " + ShellQuote(imageName) + " --format '{{.Size}}'", sourceInfo).Trim();
        long sizeBytes;
        if (long.TryParse(output, NumberStyles.Integer, CultureInfo.InvariantCulture, out sizeBytes) && sizeBytes >= 0)
        {
            return sizeBytes;
        }

        var inspectJson = ExecuteCommand(sourceClient, "docker image inspect " + ShellQuote(imageName), sourceInfo);
        var parsed = Json.DeserializeObject(inspectJson) as object[];
        if (parsed == null || parsed.Length == 0)
        {
            return 0;
        }

        var doc = parsed[0] as Dictionary<string, object>;
        if (doc == null)
        {
            return 0;
        }

        object sizeValue;
        if (doc.TryGetValue("Size", out sizeValue) &&
            sizeValue != null &&
            long.TryParse(sizeValue.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out sizeBytes) &&
            sizeBytes >= 0)
        {
            return sizeBytes;
        }

        return 0;
    }

    private static long GetRemotePathSizeBytes(SshClient sourceClient, ConnectionInfoData sourceInfo, string sourcePath)
    {
        var commandText =
            "if [ -d " + ShellQuote(sourcePath) + " ]; then " +
            "(du -sk " + ShellQuote(sourcePath) + " 2>/dev/null || du -s " + ShellQuote(sourcePath) + " 2>/dev/null) | awk 'NR==1 {print $1*1024}'; " +
            "elif [ -f " + ShellQuote(sourcePath) + " ]; then " +
            "wc -c < " + ShellQuote(sourcePath) + "; " +
            "elif [ -e " + ShellQuote(sourcePath) + " ]; then " +
            "wc -c < " + ShellQuote(sourcePath) + " 2>/dev/null || printf '0'; " +
            "else printf '0'; fi";

        var output = ExecuteCommand(sourceClient, commandText, sourceInfo).Trim();
        long sizeBytes;
        return long.TryParse(output, NumberStyles.Integer, CultureInfo.InvariantCulture, out sizeBytes) && sizeBytes >= 0
            ? sizeBytes
            : 0;
    }

    private static long CalculateEstimatedMigrationBytes(List<ContainerDefinition> definitions)
    {
        if (definitions == null || definitions.Count == 0)
        {
            return 0;
        }

        var imageBytes = definitions
            .Where(item => item != null && !string.IsNullOrWhiteSpace(item.Image))
            .GroupBy(item => item.Image, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First().EstimatedImageBytes)
            .Sum();

        var dataBytes = definitions
            .Where(item => item != null)
            .Select(item => item.EstimatedDataBytes)
            .Sum();

        return imageBytes + dataBytes;
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes <= 0)
        {
            return "0 B";
        }

        var units = new[] { "B", "KB", "MB", "GB", "TB" };
        double value = bytes;
        var unitIndex = 0;
        while (value >= 1024 && unitIndex < units.Length - 1)
        {
            value /= 1024;
            unitIndex++;
        }

        return value.ToString(value >= 10 || unitIndex == 0 ? "0" : "0.00", CultureInfo.InvariantCulture) + " " + units[unitIndex];
    }

    private static bool ImageExistsOnTarget(SshClient targetClient, ConnectionInfoData targetInfo, string imageName)
    {
        string output;
        string error;
        return TryExecuteCommand(targetClient, "docker image inspect " + ShellQuote(imageName) + " >/dev/null 2>&1", targetInfo, out output, out error);
    }

    private static bool TryPullImageOnTarget(SshClient targetClient, ConnectionInfoData targetInfo, string imageName)
    {
        string output;
        string error;
        if (TryExecuteCommand(targetClient, "docker pull " + ShellQuote(imageName), targetInfo, out output, out error))
        {
            return true;
        }

        Log("docker pull for {0} failed, falling back to archive load. {1}", imageName, BuildRemoteFailureDetails(output, error));
        return false;
    }

    private static string DownloadCommandOutputToTempFile(SshClient client, ConnectionInfoData connectionInfo, string commandText, string preferredFileName)
    {
        var tempPath = CreateLocalTransferTempFilePath(preferredFileName);
        try
        {
            DownloadCommandOutputToFile(client, connectionInfo, commandText, tempPath, false);
            return tempPath;
        }
        catch
        {
            DeleteLocalTempFile(tempPath);
            throw;
        }
    }

    private static void DownloadCommandOutputToFile(SshClient client, ConnectionInfoData connectionInfo, string commandText, string localPath, bool useSudo)
    {
        var shouldRetryWithSudo = false;
        string failureDetails = null;

        using (var fileStream = new FileStream(localPath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 128, FileOptions.SequentialScan))
        using (var cmd = client.CreateCommand(PrepareRemoteCommand(commandText, connectionInfo, useSudo)))
        {
            var async = cmd.BeginExecute();
            cmd.OutputStream.CopyTo(fileStream, 1024 * 128);
            cmd.EndExecute(async);
            fileStream.Flush();

            if (cmd.ExitStatus == 0)
            {
                return;
            }

            failureDetails = BuildRemoteFailureDetails(string.Empty, cmd.Error);
            shouldRetryWithSudo = !useSudo && ShouldRetryWithSudo(failureDetails, connectionInfo);
        }

        if (shouldRetryWithSudo)
        {
            DownloadCommandOutputToFile(client, connectionInfo, commandText, localPath, true);
            return;
        }

        throw new InvalidOperationException("Remote stream command failed: " + commandText + Environment.NewLine + failureDetails);
    }

    private static string DownloadTarGzToTempFile(SshClient sourceClient, ConnectionInfoData sourceInfo, string sourcePath, string preferredFileName)
    {
        return DownloadCommandOutputToTempFile(sourceClient, sourceInfo, BuildTarCommand(sourcePath), preferredFileName);
    }

    private static string CreateLocalTransferTempFilePath(string preferredFileName)
    {
        var baseDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "transfer-cache");
        Directory.CreateDirectory(baseDir);

        var safeName = SanitizeLocalFileName(preferredFileName);
        if (string.IsNullOrWhiteSpace(safeName))
        {
            safeName = "transfer.bin";
        }

        return Path.Combine(baseDir, Guid.NewGuid().ToString("N") + "-" + safeName);
    }

    private static string SanitizeLocalFileName(string value)
    {
        var name = (value ?? string.Empty).Trim();
        if (name.Length == 0)
        {
            return string.Empty;
        }

        foreach (var invalid in Path.GetInvalidFileNameChars())
        {
            name = name.Replace(invalid, '_');
        }

        return name;
    }

    private static void UploadLocalFile(SftpClient targetSftp, string localPath, string remotePath)
    {
        using (var fileStream = new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 128, FileOptions.SequentialScan))
        {
            targetSftp.UploadFile(fileStream, remotePath, true);
        }
    }

    private static void DeleteLocalTempFile(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }

    private static string BuildTarCommand(string sourcePath)
    {
        var normalized = sourcePath.Replace("\\", "/");
        var trimmed = normalized.TrimEnd('/');
        var lastSlash = trimmed.LastIndexOf('/');
        var parent = lastSlash > 0 ? trimmed.Substring(0, lastSlash) : "/";
        var name = lastSlash >= 0 ? trimmed.Substring(lastSlash + 1) : trimmed;
        if (name.Length == 0)
        {
            name = ".";
        }

        return "if [ -d " + ShellQuote(trimmed) + " ]; then tar -czf - -C " + ShellQuote(trimmed) + " .; " +
               "else tar -czf - -C " + ShellQuote(parent) + " " + ShellQuote(name) + "; fi";
    }

    private static void RestoreContainerData(SshClient targetClient, ConnectionInfoData targetInfo, ContainerDefinition definition, string stagingRoot)
    {
        foreach (var mount in definition.Mounts)
        {
            Log("Restoring {0}:{1}", definition.Name, mount.DestinationPath);
            var archivePath = stagingRoot.TrimEnd('/') + "/archives/" + mount.ArchiveFileName;
            if (string.Equals(mount.Type, "bind", StringComparison.OrdinalIgnoreCase))
            {
                var cmd = mount.SourceIsDirectory
                    ? "mkdir -p " + ShellQuote(mount.TargetSource) +
                      " && tar -xzf " + ShellQuote(archivePath) +
                      " -C " + ShellQuote(mount.TargetSource)
                    : "mkdir -p " + ShellQuote(GetParentDirectory(mount.TargetSource)) +
                      " && tar -xzf " + ShellQuote(archivePath) +
                      " -C " + ShellQuote(GetParentDirectory(mount.TargetSource));
                ExecuteCommand(targetClient, cmd, targetInfo);
            }
            else if (string.Equals(mount.Type, "volume", StringComparison.OrdinalIgnoreCase))
            {
                var cmd = "docker volume create " + ShellQuote(mount.VolumeName) + " >/dev/null" +
                          " && docker run --rm -v " + ShellQuote(mount.VolumeName + ":/restore") +
                          " -v " + ShellQuote(archivePath + ":/tmp/backup.tgz:ro") +
                          " alpine sh -c " + ShellQuote("cd /restore && tar -xzf /tmp/backup.tgz");
                ExecuteCommand(targetClient, cmd, targetInfo);
            }
        }
    }

    private static bool RemotePathIsDirectory(SshClient client, ConnectionInfoData connectionInfo, string path)
    {
        return RemotePathTest(client, connectionInfo, path, "-d");
    }

    private static bool RemotePathExists(SshClient client, ConnectionInfoData connectionInfo, string path)
    {
        return RemotePathTest(client, connectionInfo, path, "-e");
    }

    private static bool RemotePathTest(SshClient client, ConnectionInfoData connectionInfo, string path, string testOperator)
    {
        if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(testOperator))
        {
            return false;
        }

        var command = "[ " + testOperator + " " + ShellQuote(path) + " ] && printf 'yes' || printf 'no'";
        string output;
        string error;
        if (TryExecuteCommand(client, command, connectionInfo, out output, out error) &&
            string.Equals((output ?? string.Empty).Trim(), "yes", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (connectionInfo != null &&
            connectionInfo.Password != null &&
            TryExecuteCommandForcedSudo(client, command, connectionInfo, out output, out error))
        {
            return string.Equals((output ?? string.Empty).Trim(), "yes", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static void PrepareTargetRoot(SshClient targetClient, string targetRoot, ConnectionInfoData targetInfo)
    {
        ExecuteCommand(targetClient, "mkdir -p " + ShellQuote(targetRoot.TrimEnd('/')) + " " +
                                     ShellQuote(targetRoot.TrimEnd('/') + "/archives") + " " +
                                     ShellQuote(targetRoot.TrimEnd('/') + "/binds") + " " +
                                     ShellQuote(BuildManagedComposeDirectoryPath(targetRoot)), targetInfo);
    }

    private static string CreateStagingRoot(SshClient targetClient, ConnectionInfoData targetInfo)
    {
        var output = ExecuteCommand(targetClient, "stage=$(mktemp -d /tmp/docker-synology-migrator.XXXXXX) && mkdir -p \"$stage/archives\" \"$stage/" + ManagedComposeDirectoryName + "\" && printf '%s' \"$stage\"", targetInfo);
        return output.Trim();
    }

    private static void MigrateLegacyComposeIfNeeded(SshClient targetClient, string targetRoot, ConnectionInfoData targetInfo)
    {
        var targetComposePath = targetRoot.TrimEnd('/') + "/compose.yaml";
        var legacyComposePath = BuildManagedComposeDirectoryPath(targetRoot) + "/" + LegacyComposeFileName;
        var command =
            "if [ -f " + ShellQuote(targetComposePath) + " ] && [ ! -f " + ShellQuote(legacyComposePath) + " ]; then " +
            "mkdir -p " + ShellQuote(BuildManagedComposeDirectoryPath(targetRoot)) + " && mv " + ShellQuote(targetComposePath) + " " + ShellQuote(legacyComposePath) + " && printf 'moved'; " +
            "else printf 'noop'; fi";

        var result = ExecuteCommand(targetClient, command, targetInfo).Trim();
        if (string.Equals(result, "moved", StringComparison.OrdinalIgnoreCase))
        {
            Log("Legacy compose.yaml moved to {0}/{1}.", ManagedComposeDirectoryName, LegacyComposeFileName);
        }
    }

    private static void UploadComposeFiles(SshClient targetClient, SftpClient targetSftp, ConnectionInfoData targetInfo, string targetRoot, string stagingRoot, List<ComposeFileArtifact> composeFiles)
    {
        foreach (var composeFile in (composeFiles ?? new List<ComposeFileArtifact>())
            .Where(item => item != null && !string.IsNullOrWhiteSpace(item.FileName)))
        {
            var stagedComposePath = stagingRoot.TrimEnd('/') + "/" + composeFile.RelativePath;
            var targetComposePath = targetRoot.TrimEnd('/') + "/" + composeFile.RelativePath;
            ExecuteCommand(targetClient, "mkdir -p " + ShellQuote(GetParentDirectory(stagedComposePath)) + " " + ShellQuote(GetParentDirectory(targetComposePath)), targetInfo);
            UploadText(targetSftp, stagedComposePath, composeFile.ComposeYaml);
            ExecuteCommand(targetClient, "cp " + ShellQuote(stagedComposePath) + " " + ShellQuote(targetComposePath), targetInfo);
            Log("Compose file updated: {0}", composeFile.RelativePath);
        }
    }

    private static string BuildComposeUpCommand(string targetRoot, string serviceName, bool forceRecreate)
    {
        var serviceArg = string.IsNullOrWhiteSpace(serviceName) ? string.Empty : " " + ShellQuote(serviceName);
        var forceArg = forceRecreate ? " --force-recreate" : string.Empty;
        var composeDir = BuildManagedComposeDirectoryPath(targetRoot);
        var command =
            "cd " + ShellQuote(targetRoot) + " && " +
            "files=''; " +
            "if [ -d " + ShellQuote(composeDir) + " ]; then " +
            "for file in $(find " + ShellQuote(ManagedComposeDirectoryName) + " -maxdepth 1 -type f \\( -name '*.yaml' -o -name '*.yml' \\) | sort); do files=\"$files -f $file\"; done; " +
            "fi; " +
            "if [ -z \"$files\" ] && [ -f compose.yaml ]; then files=' -f compose.yaml'; fi; " +
            "if [ -z \"$files\" ]; then echo 'No compose files found under " + ManagedComposeDirectoryName + " or compose.yaml.' >&2; exit 1; fi; " +
            "if docker compose version >/dev/null 2>&1; then eval \"docker compose$files up -d" + forceArg + serviceArg + "\"; else eval \"docker-compose$files up -d" + forceArg + serviceArg + "\"; fi";
        return command;
    }

    private static string BuildManagedComposeDirectoryPath(string targetRoot)
    {
        return targetRoot.TrimEnd('/') + "/" + ManagedComposeDirectoryName;
    }

    private static void UploadText(SftpClient sftp, string remotePath, string content)
    {
        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(content)))
        {
            sftp.UploadFile(ms, remotePath, true);
        }
    }

    private static string BuildComposeYaml(List<ContainerDefinition> definitions)
    {
        var sb = new StringBuilder();
        sb.AppendLine("services:");
        foreach (var definition in definitions)
        {
            sb.AppendLine("  " + YamlKey(definition.Name) + ":");
            sb.AppendLine("    image: " + YamlScalar(definition.Image));
            sb.AppendLine("    container_name: " + YamlScalar(definition.Name));
            var composeNetworkMode = GetComposeNetworkMode(definition);
            var isHostNetworkMode = string.Equals(composeNetworkMode, "host", StringComparison.OrdinalIgnoreCase);

            if (!string.IsNullOrWhiteSpace(definition.Hostname))
            {
                sb.AppendLine("    hostname: " + YamlScalar(definition.Hostname));
            }

            if (!string.IsNullOrWhiteSpace(definition.User))
            {
                sb.AppendLine("    user: " + YamlScalar(definition.User));
            }

            if (!string.IsNullOrWhiteSpace(definition.WorkingDir))
            {
                sb.AppendLine("    working_dir: " + YamlScalar(definition.WorkingDir));
            }

            if (definition.Command.Count > 0)
            {
                sb.AppendLine("    command:");
                foreach (var item in definition.Command)
                {
                    sb.AppendLine("      - " + YamlScalar(item));
                }
            }

            if (definition.Entrypoint.Count > 0)
            {
                sb.AppendLine("    entrypoint:");
                foreach (var item in definition.Entrypoint)
                {
                    sb.AppendLine("      - " + YamlScalar(item));
                }
            }

            if (definition.Environment.Count > 0)
            {
                sb.AppendLine("    environment:");
                foreach (var item in definition.Environment)
                {
                    sb.AppendLine("      - " + YamlScalar(item));
                }
            }

            if (definition.Labels.Count > 0)
            {
                sb.AppendLine("    labels:");
                foreach (var kvp in definition.Labels.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                {
                    sb.AppendLine("      " + YamlKey(kvp.Key) + ": " + YamlScalar(kvp.Value));
                }
            }

            var composePortMappings = GetPortMappingsForCompose(definition);
            if (!isHostNetworkMode && composePortMappings.Count > 0)
            {
                sb.AppendLine("    ports:");
                foreach (var portMapping in composePortMappings)
                {
                    sb.AppendLine("      - " + YamlScalar(portMapping));
                }
            }

            var exposedPorts = GetExposedPortsForCompose(definition);
            if (!isHostNetworkMode && exposedPorts.Count > 0)
            {
                sb.AppendLine("    expose:");
                foreach (var port in exposedPorts)
                {
                    sb.AppendLine("      - " + YamlScalar(port));
                }
            }

            if (definition.ExtraHosts.Count > 0)
            {
                sb.AppendLine("    extra_hosts:");
                foreach (var host in definition.ExtraHosts)
                {
                    sb.AppendLine("      - " + YamlScalar(host));
                }
            }

            if (definition.CapAdd.Count > 0)
            {
                sb.AppendLine("    cap_add:");
                foreach (var cap in definition.CapAdd)
                {
                    sb.AppendLine("      - " + YamlScalar(cap));
                }
            }

            if (definition.Devices.Count > 0)
            {
                sb.AppendLine("    devices:");
                foreach (var device in definition.Devices)
                {
                    var deviceMap = device.PathOnHost + ":" + device.PathInContainer;
                    if (!string.IsNullOrWhiteSpace(device.CgroupPermissions))
                    {
                        deviceMap += ":" + device.CgroupPermissions;
                    }

                    sb.AppendLine("      - " + YamlScalar(deviceMap));
                }
            }

            if (!string.IsNullOrWhiteSpace(definition.RestartPolicyName) && !string.Equals(definition.RestartPolicyName, "no", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("    restart: " + YamlScalar(definition.RestartPolicyName));
            }

            if (definition.Privileged)
            {
                sb.AppendLine("    privileged: true");
            }

            if (definition.Tty)
            {
                sb.AppendLine("    tty: true");
            }

            if (definition.OpenStdin)
            {
                sb.AppendLine("    stdin_open: true");
            }

            if (!string.IsNullOrWhiteSpace(composeNetworkMode))
            {
                sb.AppendLine("    network_mode: " + YamlScalar(composeNetworkMode));
            }

            if (definition.Mounts.Count > 0)
            {
                sb.AppendLine("    volumes:");
                foreach (var mount in definition.Mounts)
                {
                    var source = string.Equals(mount.Type, "bind", StringComparison.OrdinalIgnoreCase)
                        ? mount.TargetSource
                        : mount.VolumeName;
                    var suffix = mount.ReadOnly ? ":ro" : string.Empty;
                    sb.AppendLine("      - " + YamlScalar(source + ":" + mount.DestinationPath + suffix));
                }
            }

            if (ShouldEmitNetworks(definition))
            {
                var attachments = GetNetworkAttachmentsForCompose(definition).ToList();
                var emitDetailedNetworkSettings = HasDetailedNetworkSettings(definition);
                sb.AppendLine("    networks:");
                foreach (var attachment in attachments)
                {
                    if (!emitDetailedNetworkSettings)
                    {
                        sb.AppendLine("      - " + YamlScalar(attachment.Name));
                        continue;
                    }

                    if (!HasDetailedNetworkSettings(attachment))
                    {
                        sb.AppendLine("      " + YamlKey(attachment.Name) + ": {}");
                        continue;
                    }

                    sb.AppendLine("      " + YamlKey(attachment.Name) + ":");

                    if (attachment.HasExplicitIpamConfiguration && !string.IsNullOrWhiteSpace(attachment.IPv4Address))
                    {
                        sb.AppendLine("        ipv4_address: " + YamlScalar(attachment.IPv4Address));
                    }

                    if (attachment.HasExplicitIpamConfiguration && !string.IsNullOrWhiteSpace(attachment.IPv6Address))
                    {
                        sb.AppendLine("        ipv6_address: " + YamlScalar(attachment.IPv6Address));
                    }

                    if (attachment.Aliases.Count > 0)
                    {
                        sb.AppendLine("        aliases:");
                        foreach (var alias in attachment.Aliases)
                        {
                            sb.AppendLine("          - " + YamlScalar(alias));
                        }
                    }
                }
            }
        }

        var volumeNames = definitions
            .SelectMany(d => d.Mounts)
            .Where(m => string.Equals(m.Type, "volume", StringComparison.OrdinalIgnoreCase))
            .Select(m => m.VolumeName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (volumeNames.Count > 0)
        {
            sb.AppendLine("volumes:");
            foreach (var volume in volumeNames)
            {
                sb.AppendLine("  " + YamlKey(volume) + ":");
            }
        }

        var networks = definitions
            .SelectMany(d => d.Networks)
            .Where(IsUserDefinedNetwork)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (networks.Count > 0)
        {
            sb.AppendLine("networks:");
            foreach (var network in networks)
            {
                sb.AppendLine("  " + YamlKey(network) + ":");
                sb.AppendLine("    external: true");
            }
        }

        return sb.ToString();
    }

    private static string YamlKey(string value)
    {
        return NeedsYamlQuotes(value) ? YamlScalar(value) : value;
    }

    private static string GetComposeNetworkMode(ContainerDefinition definition)
    {
        var mode = (definition.NetworkMode ?? string.Empty).Trim();
        if (mode.Length == 0 || string.Equals(mode, "default", StringComparison.OrdinalIgnoreCase))
        {
            return definition.Networks.Count == 1 &&
                   string.Equals(definition.Networks[0], "bridge", StringComparison.OrdinalIgnoreCase) &&
                   !HasDetailedNetworkSettings(definition)
                ? "bridge"
                : string.Empty;
        }

        if (string.Equals(mode, "host", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(mode, "none", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(mode, "bridge", StringComparison.OrdinalIgnoreCase) ||
            mode.StartsWith("container:", StringComparison.OrdinalIgnoreCase))
        {
            return mode;
        }

        return string.Empty;
    }

    private static bool ShouldEmitNetworks(ContainerDefinition definition)
    {
        return definition.Networks.Count > 0 && string.IsNullOrWhiteSpace(GetComposeNetworkMode(definition));
    }

    private static bool IsUserDefinedNetwork(string network)
    {
        if (string.IsNullOrWhiteSpace(network))
        {
            return false;
        }

        return !string.Equals(network, "bridge", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(network, "host", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(network, "none", StringComparison.OrdinalIgnoreCase);
    }

    private static string YamlScalar(string value)
    {
        value = value ?? string.Empty;
        return "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    }

    private static bool NeedsYamlQuotes(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return true;
        }

        return value.Any(ch => !char.IsLetterOrDigit(ch) && ch != '_' && ch != '-');
    }

    private static void EnsureDockerAvailable(SshClient client, ConnectionInfoData connectionInfo, string hostLabel)
    {
        string output;
        string error;
        if (TryExecuteCommand(client, "docker ps -a >/dev/null", connectionInfo, out output, out error))
        {
            return;
        }

        throw new InvalidOperationException(hostLabel + ": Docker is unavailable over SSH. " + BuildRemoteFailureDetails(output, error));
    }

    private static void EnsureComposeAvailable(SshClient client, ConnectionInfoData connectionInfo, string hostLabel)
    {
        const string composeCheck = "if docker compose version >/dev/null 2>&1; then exit 0; elif docker-compose version >/dev/null 2>&1; then exit 0; else exit 1; fi";
        string output;
        string error;
        if (TryExecuteCommand(client, composeCheck, connectionInfo, out output, out error))
        {
            return;
        }

        throw new InvalidOperationException(hostLabel + ": Docker Compose is unavailable over SSH. " + BuildRemoteFailureDetails(output, error));
    }

    private static void EnsureRemoteCommand(SshClient client, string command, ConnectionInfoData connectionInfo = null)
    {
        ExecuteCommand(client, command, connectionInfo);
    }

    private static string ExecuteCommand(SshClient client, string commandText, ConnectionInfoData connectionInfo = null)
    {
        string output;
        string error;
        if (!TryExecuteCommand(client, commandText, connectionInfo, out output, out error))
        {
            throw new InvalidOperationException("Remote command failed: " + commandText + Environment.NewLine + BuildRemoteFailureDetails(output, error));
        }

        return output;
    }

    private static bool TryExecuteCommand(SshClient client, string commandText, ConnectionInfoData connectionInfo, out string output, out string error)
    {
        using (var cmd = client.CreateCommand(PrepareRemoteCommand(commandText, connectionInfo, false)))
        {
            output = cmd.Execute() ?? string.Empty;
            error = cmd.Error ?? string.Empty;
            if (cmd.ExitStatus == 0)
            {
                return true;
            }
        }

        if (ShouldRetryWithSudo(BuildRemoteFailureDetails(output, error), connectionInfo))
        {
            Log("Permission issue detected on {0}; retrying with sudo.", connectionInfo.Host);
            using (var cmd = client.CreateCommand(PrepareRemoteCommand(commandText, connectionInfo, true)))
            {
                output = cmd.Execute() ?? string.Empty;
                error = cmd.Error ?? string.Empty;
                return cmd.ExitStatus == 0;
            }
        }

        return false;
    }

    private static bool TryExecuteCommandForcedSudo(SshClient client, string commandText, ConnectionInfoData connectionInfo, out string output, out string error)
    {
        if (connectionInfo == null || connectionInfo.Password == null)
        {
            output = string.Empty;
            error = "sudo is unavailable because SSH password is missing.";
            return false;
        }

        using (var cmd = client.CreateCommand(PrepareRemoteCommand(commandText, connectionInfo, true)))
        {
            output = cmd.Execute() ?? string.Empty;
            error = cmd.Error ?? string.Empty;
            return cmd.ExitStatus == 0;
        }
    }

    private static bool ShouldRetryWithSudo(string remoteMessage, ConnectionInfoData connectionInfo)
    {
        if (connectionInfo == null || connectionInfo.Password == null)
        {
            return false;
        }

        var text = (remoteMessage ?? string.Empty).ToLowerInvariant();
        return text.Contains("permission denied") ||
               text.Contains("must be root") ||
               text.Contains("operation not permitted") ||
               text.Contains("access denied") ||
               text.Contains("docker daemon socket");
    }

    private static string PrepareRemoteCommand(string commandText, ConnectionInfoData connectionInfo, bool useSudo)
    {
        var baseCommand =
            "export PATH=\"$PATH:/usr/local/bin:/usr/local/sbin:/usr/syno/bin:/usr/syno/sbin:" +
            "/var/packages/Docker/target/usr/bin:/var/packages/ContainerManager/target/usr/bin:" +
            "/var/packages/Virtualization/target/bin:/var/packages/Virtualization/target/sbin:" +
            "/var/packages/Virtualization/target/usr/bin:/var/packages/Virtualization/target/usr/sbin:" +
            "/var/packages/VirtualMachineManager/target/bin:/var/packages/VirtualMachineManager/target/sbin:" +
            "/var/packages/VirtualMachineManager/target/usr/bin:/var/packages/VirtualMachineManager/target/usr/sbin\"; " +
            commandText;
        if (!useSudo)
        {
            return baseCommand;
        }

        var password = ToUnsecureString(connectionInfo.Password);
        return "printf '%s\\n' " + ShellQuote(password) + " | sudo -S -p '' sh -lc " + ShellQuote(baseCommand);
    }

    private static string BuildRemoteFailureDetails(string output, string error)
    {
        var stderr = (error ?? string.Empty).Trim();
        if (stderr.Length > 0)
        {
            return stderr;
        }

        var stdout = (output ?? string.Empty).Trim();
        if (stdout.Length > 0)
        {
            return stdout;
        }

        return "The remote shell returned a non-zero exit code without stderr output.";
    }

    private static List<string> ParseDockerPsNames(string output)
    {
        return (output ?? string.Empty)
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("CONTAINER ID", StringComparison.OrdinalIgnoreCase))
            .Select(line =>
            {
                var parts = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                return parts.Length == 0 ? string.Empty : parts[parts.Length - 1];
            })
            .Where(name => name.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static Dictionary<string, object> GetDict(Dictionary<string, object> doc, string key)
    {
        object value;
        if (doc == null || !doc.TryGetValue(key, out value) || value == null)
        {
            return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        }

        return value as Dictionary<string, object> ?? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
    }

    private static object[] GetObjectArray(Dictionary<string, object> doc, string key)
    {
        object value;
        if (doc == null || !doc.TryGetValue(key, out value) || value == null)
        {
            return new object[0];
        }

        return value as object[] ?? new object[0];
    }

    private static List<string> GetStringArray(Dictionary<string, object> doc, string key)
    {
        return GetObjectArray(doc, key)
            .Select(item => item == null ? string.Empty : item.ToString())
            .Where(item => item.Length > 0)
            .ToList();
    }

    private static string GetString(Dictionary<string, object> doc, string key)
    {
        object value;
        return doc != null && doc.TryGetValue(key, out value) && value != null ? value.ToString() : null;
    }

    private static bool GetBool(Dictionary<string, object> doc, string key)
    {
        object value;
        if (doc == null || !doc.TryGetValue(key, out value) || value == null)
        {
            return false;
        }

        if (value is bool)
        {
            return (bool)value;
        }

        bool parsed;
        return bool.TryParse(value.ToString(), out parsed) && parsed;
    }

    private static string TrimSlash(string value)
    {
        return string.IsNullOrWhiteSpace(value) ? value : value.Trim('/');
    }

    private static string PathToken(string path)
    {
        var chars = path.Select(ch => char.IsLetterOrDigit(ch) ? ch : '-').ToArray();
        var token = new string(chars).Trim('-');
        return token.Length == 0 ? "data" : token;
    }

    private static string BuildManagedComposeFileName(string containerName)
    {
        return "container-" + PathToken(containerName ?? string.Empty).ToLowerInvariant() + ".yaml";
    }

    private static string GetParentDirectory(string path)
    {
        var normalized = (path ?? string.Empty).Replace("\\", "/").TrimEnd('/');
        var lastSlash = normalized.LastIndexOf('/');
        return lastSlash > 0 ? normalized.Substring(0, lastSlash) : "/";
    }

    private static string BuildUniqueName(string baseName, HashSet<string> used)
    {
        var token = PathToken(baseName).ToLowerInvariant();
        var current = token;
        var index = 1;
        while (!used.Add(current))
        {
            current = token + "-" + index.ToString(CultureInfo.InvariantCulture);
            index++;
        }

        return current;
    }

    private static string ShellQuote(string value)
    {
        return "'" + (value ?? string.Empty).Replace("'", "'\"'\"'") + "'";
    }

    private static string ToUnsecureString(SecureString value)
    {
        var ptr = IntPtr.Zero;
        try
        {
            ptr = System.Runtime.InteropServices.Marshal.SecureStringToGlobalAllocUnicode(value);
            return System.Runtime.InteropServices.Marshal.PtrToStringUni(ptr);
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                System.Runtime.InteropServices.Marshal.ZeroFreeGlobalAllocUnicode(ptr);
            }
        }
    }

    private static void Log(string format, params object[] args)
    {
        var message = "[*] " + string.Format(CultureInfo.InvariantCulture, format, args);
        var handler = LogHandler;
        if (handler != null)
        {
            handler(message);
            return;
        }

        Console.WriteLine(message);
    }
}

internal static class NetworkOverrideModes
{
    internal const string Source = "source";
    internal const string Host = "host";
    internal const string Dhcp = "dhcp";
    internal const string Static = "static";
}

internal sealed class MigrationOptions
{
    public ConnectionInfoData Source { get; set; }
    public ConnectionInfoData Target { get; set; }
    public List<string> ContainerNames { get; set; }
    public List<ContainerNetworkOverride> NetworkOverrides { get; set; }
    public string TargetRoot { get; set; }
    public bool StopContainersDuringBackup { get; set; }
    public bool DryRun { get; set; }
}

internal sealed class MigrationPlan
{
    public List<ContainerDefinition> Containers { get; set; }
    public List<ImageArchive> Images { get; set; }
    public List<ComposeFileArtifact> ComposeFiles { get; set; }
}

internal sealed class ConnectionInfoData
{
    public string Host { get; set; }
    public int Port { get; set; }
    public string Username { get; set; }
    public SecureString Password { get; set; }
}

internal sealed class ContainerNetworkOverride
{
    public string ContainerName { get; set; }
    public string Mode { get; set; }
    public string NetworkName { get; set; }
    public string IPv4Address { get; set; }
    public string IPv6Address { get; set; }
}

internal sealed class DockerNetworkCreateRequest
{
    public string Driver { get; set; }
    public string Name { get; set; }
    public string ParentInterface { get; set; }
    public string Subnet { get; set; }
    public string Gateway { get; set; }
    public string IpRange { get; set; }
    public string IpvlanMode { get; set; }
}

internal sealed class DockerNetworkDefinition
{
    public DockerNetworkDefinition()
    {
        Options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        IpamConfigs = new List<DockerNetworkIpamConfig>();
        ConnectedContainers = new List<DockerNetworkContainerReference>();
    }

    public string Id { get; set; }
    public string Name { get; set; }
    public string Driver { get; set; }
    public string Scope { get; set; }
    public bool Internal { get; set; }
    public bool Attachable { get; set; }
    public bool EnableIPv6 { get; set; }
    public string ParentInterface { get; set; }
    public string IpvlanMode { get; set; }
    public string MacvlanMode { get; set; }
    public Dictionary<string, string> Options { get; set; }
    public List<DockerNetworkIpamConfig> IpamConfigs { get; set; }
    public List<DockerNetworkContainerReference> ConnectedContainers { get; set; }
}

internal sealed class DockerNetworkIpamConfig
{
    public string Subnet { get; set; }
    public string Gateway { get; set; }
    public string IpRange { get; set; }
}

internal sealed class DockerNetworkContainerReference
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string IPv4Address { get; set; }
    public string IPv6Address { get; set; }
}

internal sealed class ContainerDefinition
{
    public ContainerDefinition()
    {
        Command = new List<string>();
        Entrypoint = new List<string>();
        Environment = new List<string>();
        ExtraHosts = new List<string>();
        CapAdd = new List<string>();
        Networks = new List<string>();
        NetworkAttachments = new List<ContainerNetworkAttachment>();
        PortBindings = new List<PortBindingItem>();
        ExposedPorts = new List<string>();
        Labels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        Mounts = new List<MountDefinition>();
        Devices = new List<DeviceMapping>();
    }

    public string Id { get; set; }
    public string Name { get; set; }
    public string Image { get; set; }
    public string Hostname { get; set; }
    public string WorkingDir { get; set; }
    public string User { get; set; }
    public string RestartPolicyName { get; set; }
    public string NetworkMode { get; set; }
    public string Status { get; set; }
    public string ComposeProject { get; set; }
    public bool Privileged { get; set; }
    public bool Tty { get; set; }
    public bool OpenStdin { get; set; }
    public bool Running { get; set; }
    public long EstimatedImageBytes { get; set; }
    public long EstimatedDataBytes { get; set; }
    public long EstimatedMigrationBytes
    {
        get { return EstimatedImageBytes + EstimatedDataBytes; }
    }
    public List<string> Command { get; set; }
    public List<string> Entrypoint { get; set; }
    public List<string> Environment { get; set; }
    public List<string> ExtraHosts { get; set; }
    public List<string> CapAdd { get; set; }
    public List<string> Networks { get; set; }
    public List<ContainerNetworkAttachment> NetworkAttachments { get; set; }
    public List<PortBindingItem> PortBindings { get; set; }
    public List<string> ExposedPorts { get; set; }
    public Dictionary<string, string> Labels { get; set; }
    public List<MountDefinition> Mounts { get; set; }
    public List<DeviceMapping> Devices { get; set; }
}

internal sealed class ContainerNetworkAttachment
{
    public ContainerNetworkAttachment()
    {
        Aliases = new List<string>();
    }

    public string Name { get; set; }
    public string IPv4Address { get; set; }
    public string IPv6Address { get; set; }
    public bool HasExplicitIpamConfiguration { get; set; }
    public List<string> Aliases { get; set; }
}

internal sealed class MountDefinition
{
    public string Type { get; set; }
    public string SourcePath { get; set; }
    public bool SourceIsDirectory { get; set; }
    public string DestinationPath { get; set; }
    public bool ReadOnly { get; set; }
    public string TargetSource { get; set; }
    public string VolumeName { get; set; }
    public string ArchiveFileName { get; set; }
    public string SafeName { get; set; }
}

internal sealed class PortBindingItem
{
    public string ContainerPort { get; set; }
    public string HostIp { get; set; }
    public string HostPort { get; set; }
}

internal sealed class DeviceMapping
{
    public string PathOnHost { get; set; }
    public string PathInContainer { get; set; }
    public string CgroupPermissions { get; set; }
}

internal sealed class ImageArchive
{
    public string Image { get; set; }
    public string ArchiveFileName { get; set; }
}

internal sealed class ComposeFileArtifact
{
    public string ContainerName { get; set; }
    public string FileName { get; set; }
    public string RelativePath { get; set; }
    public string ComposeYaml { get; set; }
}

internal sealed class VirtualMachineMigrationOptions
{
    public ConnectionInfoData Source { get; set; }
    public ConnectionInfoData Target { get; set; }
    public List<string> VirtualMachineNames { get; set; }
    public string TargetRoot { get; set; }
    public string TargetStorage { get; set; }
    public string TargetBridge { get; set; }
    public bool StopVirtualMachinesDuringExport { get; set; }
    public bool StartImportedVirtualMachines { get; set; }
    public bool DryRun { get; set; }
}

internal sealed class VirtualMachineMigrationPlan
{
    public List<VirtualMachineDefinition> VirtualMachines { get; set; }
    public string TargetStorage { get; set; }
    public string TargetBridge { get; set; }
}

internal sealed class VirtualMachineDefinition
{
    public VirtualMachineDefinition()
    {
        Disks = new List<VirtualMachineDiskDefinition>();
        Interfaces = new List<VirtualMachineInterfaceDefinition>();
    }

    public string Name { get; set; }
    public string DisplayName { get; set; }
    public string Uuid { get; set; }
    public string State { get; set; }
    public bool Running { get; set; }
    public bool Autostart { get; set; }
    public int Vcpus { get; set; }
    public long MemoryKiB { get; set; }
    public string OsType { get; set; }
    public string MachineType { get; set; }
    public bool UsesUefi { get; set; }
    public int? AssignedTargetVmId { get; set; }
    public List<VirtualMachineDiskDefinition> Disks { get; set; }
    public List<VirtualMachineInterfaceDefinition> Interfaces { get; set; }
    public long EstimatedTransferBytes
    {
        get
        {
            return Disks.Sum(item =>
            {
                if (item == null)
                {
                    return 0L;
                }

                var bytes = item.ActualSizeBytes > 0 ? item.ActualSizeBytes : item.VirtualSizeBytes;
                return Math.Max(0, bytes);
            });
        }
    }
}

internal sealed class VirtualMachineDiskDefinition
{
    public string TargetName { get; set; }
    public string DeviceType { get; set; }
    public string SourcePath { get; set; }
    public string SourcePool { get; set; }
    public string SourceVolume { get; set; }
    public string Format { get; set; }
    public long VirtualSizeBytes { get; set; }
    public long ActualSizeBytes { get; set; }
}

internal sealed class VirtualMachineInterfaceDefinition
{
    public string InterfaceName { get; set; }
    public string Type { get; set; }
    public string SourceName { get; set; }
    public string Model { get; set; }
    public string MacAddress { get; set; }
}

internal sealed class ProxmoxVirtualMachineDefinition
{
    public ProxmoxVirtualMachineDefinition()
    {
        Networks = new List<string>();
        RawConfigLines = new List<string>();
    }

    public int VmId { get; set; }
    public string Name { get; set; }
    public string Status { get; set; }
    public bool Running { get; set; }
    public int Cores { get; set; }
    public long MemoryMb { get; set; }
    public List<string> Networks { get; set; }
    public List<string> RawConfigLines { get; set; }
}
