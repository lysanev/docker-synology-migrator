using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Globalization;
using System.Net;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;
using System.Web.Script.Serialization;

internal static class Program
{
    internal const string AppVersion = "2026.04.10.23";

    [STAThread]
    private static void Main(string[] args)
    {
        MigratorCore.Initialize();

        if (args.Any(a => a.Equals("--vm-selfcheck", StringComparison.OrdinalIgnoreCase)))
        {
            Environment.ExitCode = RunVirtualMachineSelfCheck(args);
            return;
        }

        if (args.Any(a => a.Equals("--help", StringComparison.OrdinalIgnoreCase) || a.Equals("-h", StringComparison.OrdinalIgnoreCase)))
        {
            Console.WriteLine("Docker Synology Migrator v" + AppVersion);
            Console.WriteLine("GUI utility for migrating Docker containers to Debian and virtual machines to Proxmox over SSH.");
            Console.WriteLine("Run without arguments to open the interface.");
            Console.WriteLine("Use --vm-selfcheck [--profile <path>] to run VM inventory self-check via saved profile.");
            return;
        }

        Application.EnableVisualStyles();
        Application.SetCompatibleTextRenderingDefault(false);
        Application.Run(new MainForm());
    }

    private static int RunVirtualMachineSelfCheck(string[] args)
    {
        var profilePath = ResolveProfilePath(args);
        var transcript = new StringBuilder();
        Action<string> write = line =>
        {
            transcript.AppendLine(line ?? string.Empty);
            Console.WriteLine(line);
        };

        write("Docker Synology Migrator v" + AppVersion + " VM self-check");
        write("Profile: " + profilePath);

        if (!File.Exists(profilePath))
        {
            write("[ERROR] Profile file not found.");
            TryWriteSelfCheckLog(transcript.ToString());
            return 2;
        }

        try
        {
            var serializer = new JavaScriptSerializer();
            var profileJson = File.ReadAllText(profilePath, Encoding.UTF8);
            var profile = serializer.Deserialize<ConnectionProfile>(profileJson) ?? new ConnectionProfile();

            var source = BuildConnectionInfoFromProfile(
                profile.VmSourceHost,
                profile.VmSourcePort,
                profile.VmSourceLogin,
                profile.VmSourcePassword,
                "VM Synology");

            MigratorCore.LogHandler = message => write(DateTime.Now.ToString("HH:mm:ss") + " " + message);
            var definitions = MigratorCore.DiscoverVirtualMachines(source) ?? new List<VirtualMachineDefinition>();

            write(string.Empty);
            write("VM inventory:");
            foreach (var definition in definitions.OrderBy(item => item.DisplayName ?? item.Name, StringComparer.OrdinalIgnoreCase))
            {
                var displayName = string.IsNullOrWhiteSpace(definition.DisplayName) ? (definition.Name ?? string.Empty) : definition.DisplayName;
                write("- " + displayName +
                      " | state=" + (definition.State ?? "unknown") +
                      " | cpu=" + definition.Vcpus.ToString(CultureInfo.InvariantCulture) +
                      " | memGiB=" + FormatGiBFromKiB(definition.MemoryKiB) +
                      " | disks=" + definition.Disks.Count.ToString(CultureInfo.InvariantCulture) +
                      " | sizeGiB=" + FormatGiB(definition.EstimatedTransferBytes));

                foreach (var disk in definition.Disks ?? new List<VirtualMachineDiskDefinition>())
                {
                    var sourceToken = !string.IsNullOrWhiteSpace(disk.SourcePath)
                        ? disk.SourcePath
                        : ((disk.SourcePool ?? "-") + "/" + (disk.SourceVolume ?? "-"));
                    var bytes = disk.ActualSizeBytes > 0 ? disk.ActualSizeBytes : disk.VirtualSizeBytes;
                    write("  * " + (disk.TargetName ?? "disk") + " | " + sourceToken + " | sizeGiB=" + FormatGiB(bytes));
                }
            }

            var noDiskCount = definitions.Count(item => item.Disks == null || item.Disks.Count == 0);
            var zeroSizeCount = definitions.Count(item => item.EstimatedTransferBytes <= 0);
            write(string.Empty);
            write("Summary: total=" + definitions.Count.ToString(CultureInfo.InvariantCulture) +
                  ", no-disks=" + noDiskCount.ToString(CultureInfo.InvariantCulture) +
                  ", zero-size=" + zeroSizeCount.ToString(CultureInfo.InvariantCulture));
            TryWriteSelfCheckLog(transcript.ToString());
            return 0;
        }
        catch (Exception ex)
        {
            write("[ERROR] " + ex.Message);
            TryWriteSelfCheckLog(transcript.ToString());
            return 1;
        }
        finally
        {
            MigratorCore.LogHandler = null;
        }
    }

    private static void TryWriteSelfCheckLog(string content)
    {
        try
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "vm-selfcheck-last.log");
            File.WriteAllText(path, content ?? string.Empty, Encoding.UTF8);
        }
        catch
        {
        }
    }

    private static string ResolveProfilePath(string[] args)
    {
        for (var i = 0; i < (args ?? new string[0]).Length - 1; i++)
        {
            if (string.Equals(args[i], "--profile", StringComparison.OrdinalIgnoreCase))
            {
                var value = args[i + 1];
                if (!string.IsNullOrWhiteSpace(value))
                {
                    return Path.GetFullPath(value.Trim());
                }
            }
        }

        return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "connection-profile.json");
    }

    private static ConnectionInfoData BuildConnectionInfoFromProfile(string host, string port, string login, string password, string sideName)
    {
        var normalizedHost = NormalizeSavedValue(host);
        var normalizedPort = NormalizeSavedValue(port) ?? "22";
        var normalizedLogin = NormalizeSavedValue(login);
        var normalizedPassword = password ?? string.Empty;

        if (string.IsNullOrWhiteSpace(normalizedHost))
        {
            throw new InvalidOperationException(sideName + " host is required in profile.");
        }

        int parsedPort;
        if (!int.TryParse(normalizedPort, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsedPort) || parsedPort <= 0)
        {
            throw new InvalidOperationException(sideName + " SSH port in profile is invalid.");
        }

        if (string.IsNullOrWhiteSpace(normalizedLogin))
        {
            throw new InvalidOperationException(sideName + " login is required in profile.");
        }

        if (string.IsNullOrWhiteSpace(normalizedPassword))
        {
            throw new InvalidOperationException(sideName + " password is empty in profile. Enable password saving in GUI first.");
        }

        return new ConnectionInfoData
        {
            Host = normalizedHost,
            Port = parsedPort,
            Username = normalizedLogin,
            Password = ToSecureString(normalizedPassword)
        };
    }

    private static SecureString ToSecureString(string value)
    {
        var secure = new SecureString();
        foreach (var ch in value ?? string.Empty)
        {
            secure.AppendChar(ch);
        }

        secure.MakeReadOnly();
        return secure;
    }

    private static string NormalizeSavedValue(string value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string FormatGiB(long bytes)
    {
        if (bytes <= 0)
        {
            return "0.00";
        }

        var gib = bytes / 1024D / 1024D / 1024D;
        return gib.ToString("0.00", CultureInfo.InvariantCulture);
    }

    private static string FormatGiBFromKiB(long kib)
    {
        if (kib <= 0)
        {
            return "0.00";
        }

        var gib = kib / 1024D / 1024D;
        return gib.ToString("0.00", CultureInfo.InvariantCulture);
    }
}

internal sealed class MainForm : Form
{
    private readonly TextBox _sourceHostTextBox = new TextBox();
    private readonly TextBox _sourcePortTextBox = new TextBox();
    private readonly TextBox _sourceUserTextBox = new TextBox();
    private readonly TextBox _sourcePasswordTextBox = new TextBox();
    private readonly TextBox _targetHostTextBox = new TextBox();
    private readonly TextBox _targetPortTextBox = new TextBox();
    private readonly TextBox _targetUserTextBox = new TextBox();
    private readonly TextBox _targetPasswordTextBox = new TextBox();
    private readonly TextBox _targetRootTextBox = new TextBox();
    private readonly CheckBox _stopSourceContainersCheckBox = new CheckBox();
    private readonly CheckBox _dryRunCheckBox = new CheckBox();
    private readonly CheckBox _savePasswordsCheckBox = new CheckBox();
    private readonly Button _virtualMachinesScreenButton = new Button();
    private readonly Button _loadContainersButton = new Button();
    private readonly Button _selectAllButton = new Button();
    private readonly Button _selectRelatedButton = new Button();
    private readonly Button _clearSelectionButton = new Button();
    private readonly Button _startMigrationButton = new Button();
    private readonly ListView _containerListView = new ListView();
    private readonly ComboBox _networkModeComboBox = new ComboBox();
    private readonly ComboBox _networkNameComboBox = new ComboBox();
    private readonly TextBox _networkIpv4TextBox = new TextBox();
    private readonly TextBox _networkIpv6TextBox = new TextBox();
    private readonly Button _refreshNetworksButton = new Button();
    private readonly TextBox _createNetworkNameTextBox = new TextBox();
    private readonly TextBox _createParentInterfaceTextBox = new TextBox();
    private readonly TextBox _createSubnetTextBox = new TextBox();
    private readonly TextBox _createGatewayTextBox = new TextBox();
    private readonly TextBox _createIpRangeTextBox = new TextBox();
    private readonly ComboBox _createIpvlanModeComboBox = new ComboBox();
    private readonly TextBox _macvlanCommandTextBox = new TextBox();
    private readonly TextBox _ipvlanCommandTextBox = new TextBox();
    private readonly Button _copyMacvlanButton = new Button();
    private readonly Button _copyIpvlanButton = new Button();
    private readonly Button _createMacvlanOnDebianButton = new Button();
    private readonly Button _createIpvlanOnDebianButton = new Button();
    private readonly Button _createAndUseMacvlanOnDebianButton = new Button();
    private readonly Button _createAndUseIpvlanOnDebianButton = new Button();
    private readonly Button _useCreatedNetworkButton = new Button();
    private readonly Button _refreshNetworkInventoryButton = new Button();
    private readonly Button _deleteNetworkButton = new Button();
    private readonly Button _useSelectedNetworkButton = new Button();
    private readonly ListView _networkInventoryListView = new ListView();
    private readonly TextBox _networkInventoryDetailsTextBox = new TextBox();
    private readonly Button _loadTargetContainersButton = new Button();
    private readonly Button _startTargetContainerButton = new Button();
    private readonly Button _stopTargetContainerButton = new Button();
    private readonly Button _deleteTargetContainerButton = new Button();
    private readonly Button _applyTargetContainerNetworkButton = new Button();
    private readonly Button _refreshTargetContainerNetworksButton = new Button();
    private readonly ListView _targetContainerListView = new ListView();
    private readonly TextBox _targetContainerDetailsTextBox = new TextBox();
    private readonly ComboBox _targetContainerNetworkModeComboBox = new ComboBox();
    private readonly ComboBox _targetContainerNetworkNameComboBox = new ComboBox();
    private readonly TextBox _targetContainerNetworkIpv4TextBox = new TextBox();
    private readonly TextBox _targetContainerNetworkIpv6TextBox = new TextBox();
    private readonly TextBox _detailsTextBox = new TextBox();
    private readonly RichTextBox _logTextBox = new RichTextBox();
    private readonly Label _statusLabel = new Label();
    private readonly Label _selectionLabel = new Label();
    private readonly Label _logPathLabel = new Label();
    private readonly Label _sourceNoteLabel = new Label();
    private readonly Label _legendLabel = new Label();
    private readonly Label _networkHintLabel = new Label();
    private readonly Label _targetContainerNetworkHintLabel = new Label();
    private readonly ProgressBar _progressBar = new ProgressBar();
    private readonly SplitContainer _workspaceSplitContainer = new SplitContainer();
    private readonly SplitContainer _mainSplitContainer = new SplitContainer();
    private readonly SplitContainer _networkInventorySplitContainer = new SplitContainer();
    private readonly SplitContainer _targetContainerSplitContainer = new SplitContainer();
    private readonly string _profilePath;
    private readonly Dictionary<string, long> _migrationContainerBytes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _migrationImageBytes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, double> _migrationContainerFractions = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, double> _migrationImageFractions = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _completedMigrationContainers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _completedMigrationImages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private readonly List<ContainerViewModel> _containers = new List<ContainerViewModel>();
    private readonly List<string> _targetDockerNetworks = new List<string>();
    private readonly List<DockerNetworkDefinition> _targetDockerNetworkDefinitions = new List<DockerNetworkDefinition>();
    private readonly List<ContainerDefinition> _targetContainerDefinitions = new List<ContainerDefinition>();
    private SessionLogger _logger;
    private bool _busy;
    private bool _suppressItemCheckedEvents;
    private bool _suppressNetworkEditorEvents;
    private bool _suppressTargetContainerNetworkEditorEvents;
    private string _activeNetworkEditorContainerName;
    private string _busyStatusText;
    private string _lastProgressMessage;
    private bool _migrationProgressActive;
    private DateTime _migrationStartedUtc;
    private long _migrationTotalBytes;
    private long _migrationCompletedBytes;
    private ConnectionProfile _pendingLayoutProfile;
    private bool _savedLayoutApplied;
    private Font _listFontRegular;
    private Font _listFontBold;

    private static readonly Color AppBackColor = Color.FromArgb(245, 242, 235);
    private static readonly Color SurfaceColor = Color.FromArgb(255, 252, 247);
    private static readonly Color AccentColor = Color.FromArgb(33, 110, 122);
    private static readonly Color AccentDarkColor = Color.FromArgb(22, 78, 87);
    private static readonly Color SecondaryButtonColor = Color.FromArgb(233, 226, 213);
    private static readonly Color SafeSelectionColor = Color.FromArgb(223, 242, 236);
    private static readonly Color RelatedSelectionColor = Color.FromArgb(247, 234, 205);
    private static readonly Color RiskSelectionColor = Color.FromArgb(250, 212, 167);
    private static readonly Color TextColor = Color.FromArgb(36, 42, 44);
    private static readonly JavaScriptSerializer Json = new JavaScriptSerializer();
    private const string SourceNetworkModeLabel = "Source configuration";
    private const string HostNetworkModeLabel = "Host";
    private const string DhcpNetworkModeLabel = "DHCP";
    private const string StaticNetworkModeLabel = "Static IP";
    private const string CreatorDefaultsHint = "Fill in network name, parent, and subnet to get ready-to-run commands.";

    internal MainForm()
    {
        Text = BuildAppTitle();
        StartPosition = FormStartPosition.CenterScreen;
        MinimumSize = new Size(1240, 820);
        Size = new Size(1440, 940);
        BackColor = AppBackColor;
        ForeColor = TextColor;
        Font = new Font("Segoe UI", 9.5F, FontStyle.Regular, GraphicsUnit.Point);
        try
        {
            Icon = Icon.ExtractAssociatedIcon(Application.ExecutablePath);
        }
        catch
        {
        }
        _profilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "connection-profile.json");
        FormClosing += MainFormFormClosing;
        Shown += MainFormShown;

        BuildLayout();
        WireCredentialFieldEvents();
        ApplyTheme();
        InitializeDefaults();
        InitializeLogger();
        LoadSavedProfile();
        UpdateSelectionSummary();
    }

    private void BuildLayout()
    {
        var root = new TableLayoutPanel();
        root.Dock = DockStyle.Fill;
        root.Padding = new Padding(14, 12, 14, 14);
        root.ColumnCount = 1;
        root.RowCount = 4;
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 76F));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 156F));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 90F));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));
        Controls.Add(root);

        root.Controls.Add(BuildHeaderPanel(), 0, 0);
        root.Controls.Add(BuildConnectionPanel(), 0, 1);
        root.Controls.Add(BuildActionPanel(), 0, 2);
        root.Controls.Add(BuildWorkspacePanel(), 0, 3);
    }

    private Control BuildWorkspacePanel()
    {
        _workspaceSplitContainer.Dock = DockStyle.Fill;
        _workspaceSplitContainer.Orientation = Orientation.Horizontal;
        _workspaceSplitContainer.SplitterWidth = 7;
        ConfigureInitialSplitterDistance(_workspaceSplitContainer, 0, 116);

        _workspaceSplitContainer.Panel1.Controls.Add(BuildMainPanel());
        _workspaceSplitContainer.Panel2.Controls.Add(BuildLogPanel());
        return _workspaceSplitContainer;
    }

    private void WireCredentialFieldEvents()
    {
        _targetHostTextBox.TextChanged += TargetConnectionFieldsChanged;
        _targetPortTextBox.TextChanged += TargetConnectionFieldsChanged;
        _targetUserTextBox.TextChanged += TargetConnectionFieldsChanged;
        _targetPasswordTextBox.TextChanged += TargetConnectionFieldsChanged;
    }

    private Control BuildHeaderPanel()
    {
        var panel = new Panel();
        panel.Dock = DockStyle.Fill;
        panel.Margin = new Padding(0, 0, 0, 10);
        panel.Padding = new Padding(20, 14, 20, 10);
        panel.BackColor = AccentDarkColor;
        panel.Tag = "header";

        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 2;
        layout.RowCount = 1;
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 170F));

        var title = new Label();
        title.Dock = DockStyle.Fill;
        title.ForeColor = Color.White;
        title.Font = new Font("Segoe UI Semibold", 18F, FontStyle.Bold, GraphicsUnit.Point);
        title.Text = BuildAppTitle();
        title.TextAlign = ContentAlignment.MiddleLeft;

        _virtualMachinesScreenButton.Text = "Virtual Machines";
        _virtualMachinesScreenButton.Dock = DockStyle.Right;
        _virtualMachinesScreenButton.Margin = new Padding(0, 8, 0, 8);
        _virtualMachinesScreenButton.Click += OpenVirtualMachinesScreenButtonClick;

        layout.Controls.Add(title, 0, 0);
        layout.Controls.Add(_virtualMachinesScreenButton, 1, 0);
        panel.Controls.Add(layout);
        return panel;
    }

    private Control BuildConnectionPanel()
    {
        var panel = new TableLayoutPanel();
        panel.Dock = DockStyle.Fill;
        panel.ColumnCount = 3;
        panel.RowCount = 1;
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 33.34F));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 33.33F));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 33.33F));

        panel.Controls.Add(BuildConnectionGroup("Synology Source", _sourceHostTextBox, _sourcePortTextBox, _sourceUserTextBox, _sourcePasswordTextBox), 0, 0);
        panel.Controls.Add(BuildConnectionGroup("Debian Target", _targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox), 1, 0);
        panel.Controls.Add(BuildOptionsGroup(), 2, 0);
        return panel;
    }

    private Control BuildConnectionGroup(string title, TextBox hostTextBox, TextBox portTextBox, TextBox userTextBox, TextBox passwordTextBox)
    {
        var group = new GroupBox();
        group.Text = title;
        group.Dock = DockStyle.Fill;

        var table = new TableLayoutPanel();
        table.Dock = DockStyle.Fill;
        table.ColumnCount = 2;
        table.RowCount = 4;
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        for (var i = 0; i < 4; i++)
        {
            table.RowStyles.Add(new RowStyle(SizeType.Absolute, 30F));
        }

        passwordTextBox.UseSystemPasswordChar = true;

        AddField(table, 0, "Host", hostTextBox);
        AddField(table, 1, "Port", portTextBox);
        AddField(table, 2, "Login", userTextBox);
        AddField(table, 3, "Password", passwordTextBox);

        group.Controls.Add(table);
        return group;
    }

    private Control BuildOptionsGroup()
    {
        var group = new GroupBox();
        group.Text = "Migration Options";
        group.Dock = DockStyle.Fill;

        var table = new TableLayoutPanel();
        table.Dock = DockStyle.Fill;
        table.ColumnCount = 1;
        table.RowCount = 6;
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 30F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 26F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 26F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 26F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 24F));
        table.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        var targetRootPanel = new TableLayoutPanel();
        targetRootPanel.Dock = DockStyle.Fill;
        targetRootPanel.ColumnCount = 2;
        targetRootPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        targetRootPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        targetRootPanel.Controls.Add(BuildLabel("Target path"), 0, 0);
        targetRootPanel.Controls.Add(_targetRootTextBox, 1, 0);

        _stopSourceContainersCheckBox.Text = "Temporarily stop source containers during backup";
        _stopSourceContainersCheckBox.AutoSize = true;

        _dryRunCheckBox.Text = "Dry Run: validate and build plan only";
        _dryRunCheckBox.AutoSize = true;
        _dryRunCheckBox.CheckedChanged += DryRunCheckBoxChanged;

        _savePasswordsCheckBox.Text = "Save passwords in profile";
        _savePasswordsCheckBox.AutoSize = true;

        _sourceNoteLabel.Text = "Source containers are never deleted automatically. In Dry Run mode nothing is copied or changed.";
        _sourceNoteLabel.Dock = DockStyle.Fill;
        _sourceNoteLabel.ForeColor = Color.FromArgb(64, 64, 64);

        _logPathLabel.Dock = DockStyle.Fill;
        _logPathLabel.AutoEllipsis = true;
        _logPathLabel.TextAlign = ContentAlignment.MiddleLeft;
        _logPathLabel.Font = new Font("Segoe UI", 8.5F, FontStyle.Regular, GraphicsUnit.Point);

        table.Controls.Add(targetRootPanel, 0, 0);
        table.Controls.Add(_stopSourceContainersCheckBox, 0, 1);
        table.Controls.Add(_dryRunCheckBox, 0, 2);
        table.Controls.Add(_savePasswordsCheckBox, 0, 3);
        table.Controls.Add(_logPathLabel, 0, 4);
        table.Controls.Add(_sourceNoteLabel, 0, 5);

        group.Controls.Add(table);
        return group;
    }

    private Control BuildActionPanel()
    {
        var panel = new TableLayoutPanel();
        panel.Dock = DockStyle.Fill;
        panel.ColumnCount = 1;
        panel.RowCount = 3;
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 20F));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));

        var buttonsPanel = new FlowLayoutPanel();
        buttonsPanel.Dock = DockStyle.Fill;
        buttonsPanel.FlowDirection = FlowDirection.LeftToRight;
        buttonsPanel.WrapContents = false;

        _loadContainersButton.Text = "Load Containers";
        _selectAllButton.Text = "Select All";
        _selectRelatedButton.Text = "Select Related";
        _clearSelectionButton.Text = "Clear Selection";
        _startMigrationButton.Text = "Start Migration";

        _loadContainersButton.Click += LoadContainersButtonClick;
        _selectAllButton.Click += SelectAllButtonClick;
        _selectRelatedButton.Click += SelectRelatedButtonClick;
        _clearSelectionButton.Click += ClearSelectionButtonClick;
        _startMigrationButton.Click += StartMigrationButtonClick;

        buttonsPanel.Controls.Add(_loadContainersButton);
        buttonsPanel.Controls.Add(_selectAllButton);
        buttonsPanel.Controls.Add(_selectRelatedButton);
        buttonsPanel.Controls.Add(_clearSelectionButton);
        buttonsPanel.Controls.Add(_startMigrationButton);

        _legendLabel.Dock = DockStyle.Fill;
        _legendLabel.TextAlign = ContentAlignment.MiddleLeft;
        _legendLabel.ForeColor = Color.FromArgb(98, 87, 61);
        _legendLabel.Text = "Legend: green = ready, amber = selected with missing related containers, sand = related container not selected.";

        var statusPanel = new TableLayoutPanel();
        statusPanel.Dock = DockStyle.Fill;
        statusPanel.ColumnCount = 3;
        statusPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 420F));
        statusPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 340F));
        statusPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));

        _selectionLabel.Dock = DockStyle.Fill;
        _selectionLabel.TextAlign = ContentAlignment.MiddleLeft;
        _selectionLabel.AutoEllipsis = true;

        _progressBar.Dock = DockStyle.Fill;
        _progressBar.Style = ProgressBarStyle.Blocks;

        _statusLabel.Dock = DockStyle.Fill;
        _statusLabel.TextAlign = ContentAlignment.MiddleLeft;
        _statusLabel.AutoEllipsis = true;

        statusPanel.Controls.Add(_selectionLabel, 0, 0);
        statusPanel.Controls.Add(_progressBar, 1, 0);
        statusPanel.Controls.Add(_statusLabel, 2, 0);

        panel.Controls.Add(buttonsPanel, 0, 0);
        panel.Controls.Add(_legendLabel, 0, 1);
        panel.Controls.Add(statusPanel, 0, 2);
        return panel;
    }

    private Control BuildMainPanel()
    {
        _mainSplitContainer.Dock = DockStyle.Fill;
        ConfigureInitialSplitterDistance(_mainSplitContainer, 760, 0);

        var leftGroup = new GroupBox();
        leftGroup.Text = "Containers";
        leftGroup.Dock = DockStyle.Fill;

        _containerListView.Dock = DockStyle.Fill;
        _containerListView.View = View.Details;
        _containerListView.CheckBoxes = true;
        _containerListView.FullRowSelect = true;
        _containerListView.HideSelection = false;
        _containerListView.MultiSelect = false;
        _containerListView.GridLines = false;
        _containerListView.Columns.Add("Name", 170);
        _containerListView.Columns.Add("State", 90);
        _containerListView.Columns.Add("Image", 190);
        _containerListView.Columns.Add("Size (GB)", 90);
        _containerListView.Columns.Add("Group", 120);
        _containerListView.Columns.Add("Networks", 120);
        _containerListView.Columns.Add("Related", 180);
        _containerListView.SelectedIndexChanged += ContainerSelectionChanged;
        _containerListView.ItemChecked += ContainerCheckedChanged;
        leftGroup.Controls.Add(_containerListView);

        var rightLayout = new TableLayoutPanel();
        rightLayout.Dock = DockStyle.Fill;
        rightLayout.ColumnCount = 1;
        rightLayout.RowCount = 2;
        rightLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 160F));
        rightLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));
        rightLayout.Controls.Add(BuildNetworkEditorGroup(), 0, 0);
        rightLayout.Controls.Add(BuildNetworkCommandsGroup(), 0, 1);

        _mainSplitContainer.Panel1.Controls.Add(leftGroup);
        _mainSplitContainer.Panel2.Controls.Add(rightLayout);
        return _mainSplitContainer;
    }

    private Control BuildNetworkEditorGroup()
    {
        var group = new GroupBox();
        group.Text = "Migration Network";
        group.Dock = DockStyle.Fill;

        var table = new TableLayoutPanel();
        table.Dock = DockStyle.Fill;
        table.ColumnCount = 2;
        table.RowCount = 5;
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 118F));
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        _networkModeComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
        _networkNameComboBox.DropDownStyle = ComboBoxStyle.DropDown;
        _networkModeComboBox.Items.AddRange(new object[]
        {
            SourceNetworkModeLabel,
            HostNetworkModeLabel,
            DhcpNetworkModeLabel,
            StaticNetworkModeLabel
        });
        _networkModeComboBox.SelectedIndexChanged += NetworkEditorChanged;
        _networkNameComboBox.TextChanged += NetworkEditorChanged;
        _networkIpv4TextBox.TextChanged += NetworkEditorChanged;
        _networkIpv6TextBox.TextChanged += NetworkEditorChanged;

        _refreshNetworksButton.Text = "Debian Networks";
        _refreshNetworksButton.AutoSize = true;
        _refreshNetworksButton.Click += RefreshTargetNetworksButtonClick;

        var networkPickerPanel = new TableLayoutPanel();
        networkPickerPanel.Dock = DockStyle.Fill;
        networkPickerPanel.ColumnCount = 2;
        networkPickerPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        networkPickerPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 112F));
        networkPickerPanel.Controls.Add(_networkNameComboBox, 0, 0);
        networkPickerPanel.Controls.Add(_refreshNetworksButton, 1, 0);

        _networkHintLabel.Dock = DockStyle.Fill;
        _networkHintLabel.ForeColor = Color.FromArgb(96, 86, 66);
        _networkHintLabel.Text = "Select a container on the left. For DHCP and Static IP modes, choose a target Debian network.";

        AddField(table, 0, "Mode", _networkModeComboBox);
        AddField(table, 1, "Network", networkPickerPanel);
        AddField(table, 2, "IPv4", _networkIpv4TextBox);
        AddField(table, 3, "IPv6", _networkIpv6TextBox);
        table.Controls.Add(_networkHintLabel, 0, 4);
        table.SetColumnSpan(_networkHintLabel, 2);

        group.Controls.Add(table);
        return group;
    }

    private Control BuildNetworkCommandsGroup()
    {
        var group = new GroupBox();
        group.Text = "Debian Target";
        group.Dock = DockStyle.Fill;

        var tabs = new TabControl();
        tabs.Dock = DockStyle.Fill;

        var targetContainersTab = new TabPage("Target Containers");
        targetContainersTab.Controls.Add(BuildTargetDetailsTab());

        var targetNetworkTab = new TabPage("Target Network");
        targetNetworkTab.Controls.Add(BuildTargetContainerNetworkTab());

        var inventoryTab = new TabPage("Networks");
        inventoryTab.Controls.Add(BuildNetworkInventoryTab());

        var createTab = new TabPage("Create Network");
        createTab.Controls.Add(BuildNetworkCreateTab());

        var sourceTab = new TabPage("Source Details");
        sourceTab.Controls.Add(BuildSourceDetailsTab());

        tabs.TabPages.Add(targetContainersTab);
        tabs.TabPages.Add(targetNetworkTab);
        tabs.TabPages.Add(inventoryTab);
        tabs.TabPages.Add(createTab);
        tabs.TabPages.Add(sourceTab);
        group.Controls.Add(tabs);
        return group;
    }

    private Control BuildNetworkInventoryTab()
    {
        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 1;
        layout.RowCount = 2;
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 42F));
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        var buttons = new FlowLayoutPanel();
        buttons.Dock = DockStyle.Fill;
        buttons.FlowDirection = FlowDirection.LeftToRight;
        buttons.WrapContents = false;

        _refreshNetworkInventoryButton.Text = "Refresh";
        _deleteNetworkButton.Text = "Delete";
        _useSelectedNetworkButton.Text = "Use for Migration";
        _refreshNetworkInventoryButton.Click += RefreshTargetNetworksButtonClick;
        _deleteNetworkButton.Click += DeleteNetworkButtonClick;
        _useSelectedNetworkButton.Click += UseSelectedNetworkButtonClick;

        buttons.Controls.Add(_refreshNetworkInventoryButton);
        buttons.Controls.Add(_deleteNetworkButton);
        buttons.Controls.Add(_useSelectedNetworkButton);

        _networkInventoryListView.Dock = DockStyle.Fill;
        _networkInventoryListView.View = View.Details;
        _networkInventoryListView.FullRowSelect = true;
        _networkInventoryListView.HideSelection = false;
        _networkInventoryListView.MultiSelect = false;
        _networkInventoryListView.Columns.Add("Name", 120);
        _networkInventoryListView.Columns.Add("Driver", 80);
        _networkInventoryListView.Columns.Add("Scope", 70);
        _networkInventoryListView.Columns.Add("Subnet", 130);
        _networkInventoryListView.Columns.Add("Parent", 90);
        _networkInventoryListView.SelectedIndexChanged += NetworkInventorySelectionChanged;

        _networkInventoryDetailsTextBox.Dock = DockStyle.Fill;
        _networkInventoryDetailsTextBox.Multiline = true;
        _networkInventoryDetailsTextBox.ReadOnly = true;
        _networkInventoryDetailsTextBox.ScrollBars = ScrollBars.Vertical;
        _networkInventoryDetailsTextBox.BorderStyle = BorderStyle.None;
        _networkInventoryDetailsTextBox.Font = new Font(FontFamily.GenericMonospace, 8.75F);

        _networkInventorySplitContainer.Dock = DockStyle.Fill;
        _networkInventorySplitContainer.Orientation = Orientation.Horizontal;
        _networkInventorySplitContainer.SplitterWidth = 6;
        ConfigureInitialSplitterDistance(_networkInventorySplitContainer, 0, 130);
        _networkInventorySplitContainer.Panel1.Controls.Add(_networkInventoryListView);
        _networkInventorySplitContainer.Panel2.Controls.Add(_networkInventoryDetailsTextBox);

        layout.Controls.Add(buttons, 0, 0);
        layout.Controls.Add(_networkInventorySplitContainer, 0, 1);
        return layout;
    }

    private Control BuildNetworkCreateTab()
    {
        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 1;
        layout.RowCount = 3;
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 132F));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 112F));
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        var fields = new TableLayoutPanel();
        fields.Dock = DockStyle.Fill;
        fields.ColumnCount = 4;
        fields.RowCount = 3;
        fields.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 82F));
        fields.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
        fields.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 82F));
        fields.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
        fields.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        fields.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        fields.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));

        _createIpvlanModeComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
        _createIpvlanModeComboBox.Items.AddRange(new object[] { "l2", "l3", "l3s" });
        _createIpvlanModeComboBox.SelectedIndexChanged += NetworkCreateFieldsChanged;
        _createNetworkNameTextBox.TextChanged += NetworkCreateFieldsChanged;
        _createParentInterfaceTextBox.TextChanged += NetworkCreateFieldsChanged;
        _createSubnetTextBox.TextChanged += NetworkCreateFieldsChanged;
        _createGatewayTextBox.TextChanged += NetworkCreateFieldsChanged;
        _createIpRangeTextBox.TextChanged += NetworkCreateFieldsChanged;

        AddField(fields, 0, "Name", _createNetworkNameTextBox);
        AddField(fields, 1, "Subnet", _createSubnetTextBox);
        fields.Controls.Add(BuildLabel("Parent"), 2, 0);
        _createParentInterfaceTextBox.Dock = DockStyle.Fill;
        fields.Controls.Add(_createParentInterfaceTextBox, 3, 0);
        fields.Controls.Add(BuildLabel("Gateway"), 2, 1);
        _createGatewayTextBox.Dock = DockStyle.Fill;
        fields.Controls.Add(_createGatewayTextBox, 3, 1);
        AddField(fields, 2, "IP range", _createIpRangeTextBox);
        fields.Controls.Add(BuildLabel("ipvlan"), 2, 2);
        _createIpvlanModeComboBox.Dock = DockStyle.Fill;
        fields.Controls.Add(_createIpvlanModeComboBox, 3, 2);

        var actions = new FlowLayoutPanel();
        actions.Dock = DockStyle.Fill;
        actions.FlowDirection = FlowDirection.LeftToRight;
        actions.WrapContents = true;

        _copyMacvlanButton.Text = "Copy macvlan";
        _copyIpvlanButton.Text = "Copy ipvlan";
        _createMacvlanOnDebianButton.Text = "Create macvlan";
        _createIpvlanOnDebianButton.Text = "Create ipvlan";
        _createAndUseMacvlanOnDebianButton.Text = "Create macvlan + Apply";
        _createAndUseIpvlanOnDebianButton.Text = "Create ipvlan + Apply";
        _useCreatedNetworkButton.Text = "Apply Name";
        _copyMacvlanButton.Click += CopyMacvlanButtonClick;
        _copyIpvlanButton.Click += CopyIpvlanButtonClick;
        _createMacvlanOnDebianButton.Click += CreateMacvlanOnDebianButtonClick;
        _createIpvlanOnDebianButton.Click += CreateIpvlanOnDebianButtonClick;
        _createAndUseMacvlanOnDebianButton.Click += CreateAndUseMacvlanOnDebianButtonClick;
        _createAndUseIpvlanOnDebianButton.Click += CreateAndUseIpvlanOnDebianButtonClick;
        _useCreatedNetworkButton.Click += UseCreatedNetworkButtonClick;

        actions.Controls.Add(_copyMacvlanButton);
        actions.Controls.Add(_copyIpvlanButton);
        actions.Controls.Add(_createMacvlanOnDebianButton);
        actions.Controls.Add(_createIpvlanOnDebianButton);
        actions.Controls.Add(_createAndUseMacvlanOnDebianButton);
        actions.Controls.Add(_createAndUseIpvlanOnDebianButton);
        actions.Controls.Add(_useCreatedNetworkButton);

        var previews = new TableLayoutPanel();
        previews.Dock = DockStyle.Fill;
        previews.ColumnCount = 3;
        previews.RowCount = 2;
        previews.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 72F));
        previews.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        previews.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 1F));
        previews.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));
        previews.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));

        ConfigureCommandPreview(_macvlanCommandTextBox);
        ConfigureCommandPreview(_ipvlanCommandTextBox);
        previews.Controls.Add(BuildLabel("macvlan"), 0, 0);
        previews.Controls.Add(_macvlanCommandTextBox, 1, 0);
        previews.Controls.Add(BuildLabel("ipvlan"), 0, 1);
        previews.Controls.Add(_ipvlanCommandTextBox, 1, 1);

        layout.Controls.Add(fields, 0, 0);
        layout.Controls.Add(actions, 0, 1);
        layout.Controls.Add(previews, 0, 2);
        return layout;
    }

    private Control BuildDetailsGroup()
    {
        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 1;
        layout.RowCount = 2;
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 38F));
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 62F));

        var sourceGroup = new GroupBox();
        sourceGroup.Text = "Source Container Details";
        sourceGroup.Dock = DockStyle.Fill;
        sourceGroup.Controls.Add(BuildSourceDetailsTab());

        var targetGroup = new GroupBox();
        targetGroup.Text = "Debian Target Containers";
        targetGroup.Dock = DockStyle.Fill;
        targetGroup.Controls.Add(BuildTargetDetailsTab());

        layout.Controls.Add(sourceGroup, 0, 0);
        layout.Controls.Add(targetGroup, 0, 1);
        return layout;
    }

    private Control BuildSourceDetailsTab()
    {
        _detailsTextBox.Dock = DockStyle.Fill;
        _detailsTextBox.Multiline = true;
        _detailsTextBox.ReadOnly = true;
        _detailsTextBox.ScrollBars = ScrollBars.Both;
        _detailsTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _detailsTextBox.BorderStyle = BorderStyle.None;
        return _detailsTextBox;
    }

    private Control BuildTargetDetailsTab()
    {
        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 1;
        layout.RowCount = 2;
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 42F));
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        var buttons = new FlowLayoutPanel();
        buttons.Dock = DockStyle.Fill;
        buttons.FlowDirection = FlowDirection.LeftToRight;
        buttons.WrapContents = false;

        _loadTargetContainersButton.Text = "Load Target Containers";
        _startTargetContainerButton.Text = "Start";
        _stopTargetContainerButton.Text = "Stop";
        _deleteTargetContainerButton.Text = "Delete";
        _loadTargetContainersButton.Click += LoadTargetContainersButtonClick;
        _startTargetContainerButton.Click += StartTargetContainerButtonClick;
        _stopTargetContainerButton.Click += StopTargetContainerButtonClick;
        _deleteTargetContainerButton.Click += DeleteTargetContainerButtonClick;

        buttons.Controls.Add(_loadTargetContainersButton);
        buttons.Controls.Add(_startTargetContainerButton);
        buttons.Controls.Add(_stopTargetContainerButton);
        buttons.Controls.Add(_deleteTargetContainerButton);

        _targetContainerListView.Dock = DockStyle.Fill;
        _targetContainerListView.View = View.Details;
        _targetContainerListView.FullRowSelect = true;
        _targetContainerListView.HideSelection = false;
        _targetContainerListView.MultiSelect = false;
        _targetContainerListView.Columns.Add("Name", 120);
        _targetContainerListView.Columns.Add("State", 80);
        _targetContainerListView.Columns.Add("Image", 180);
        _targetContainerListView.Columns.Add("Networks", 120);
        _targetContainerListView.Columns.Add("IP", 120);
        _targetContainerListView.SelectedIndexChanged += TargetContainerSelectionChanged;

        _targetContainerDetailsTextBox.Dock = DockStyle.Fill;
        _targetContainerDetailsTextBox.Multiline = true;
        _targetContainerDetailsTextBox.ReadOnly = true;
        _targetContainerDetailsTextBox.ScrollBars = ScrollBars.Both;
        _targetContainerDetailsTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _targetContainerDetailsTextBox.BorderStyle = BorderStyle.None;

        _targetContainerSplitContainer.Dock = DockStyle.Fill;
        _targetContainerSplitContainer.Orientation = Orientation.Horizontal;
        _targetContainerSplitContainer.SplitterWidth = 6;
        ConfigureInitialSplitterDistance(_targetContainerSplitContainer, 0, 140);
        _targetContainerSplitContainer.Panel1.Controls.Add(_targetContainerListView);
        _targetContainerSplitContainer.Panel2.Controls.Add(_targetContainerDetailsTextBox);

        layout.Controls.Add(buttons, 0, 0);
        layout.Controls.Add(_targetContainerSplitContainer, 0, 1);
        return layout;
    }

    private Control BuildTargetContainerNetworkTab()
    {
        var group = new GroupBox();
        group.Text = "Target Container Network";
        group.Dock = DockStyle.Fill;

        var table = new TableLayoutPanel();
        table.Dock = DockStyle.Fill;
        table.ColumnCount = 2;
        table.RowCount = 6;
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 118F));
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 32F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 38F));
        table.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        _targetContainerNetworkModeComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
        _targetContainerNetworkModeComboBox.Items.AddRange(new object[]
        {
            HostNetworkModeLabel,
            DhcpNetworkModeLabel,
            StaticNetworkModeLabel
        });
        _targetContainerNetworkModeComboBox.SelectedIndexChanged += TargetContainerNetworkEditorChanged;
        _targetContainerNetworkNameComboBox.DropDownStyle = ComboBoxStyle.DropDown;
        _targetContainerNetworkNameComboBox.TextChanged += TargetContainerNetworkEditorChanged;
        _targetContainerNetworkIpv4TextBox.TextChanged += TargetContainerNetworkEditorChanged;
        _targetContainerNetworkIpv6TextBox.TextChanged += TargetContainerNetworkEditorChanged;

        _refreshTargetContainerNetworksButton.Text = "Debian Networks";
        _refreshTargetContainerNetworksButton.Click += RefreshTargetNetworksButtonClick;

        var networkPickerPanel = new TableLayoutPanel();
        networkPickerPanel.Dock = DockStyle.Fill;
        networkPickerPanel.ColumnCount = 2;
        networkPickerPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        networkPickerPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 112F));
        networkPickerPanel.Controls.Add(_targetContainerNetworkNameComboBox, 0, 0);
        networkPickerPanel.Controls.Add(_refreshTargetContainerNetworksButton, 1, 0);

        _applyTargetContainerNetworkButton.Text = "Apply to Target Container";
        _applyTargetContainerNetworkButton.Click += ApplyTargetContainerNetworkButtonClick;

        _targetContainerNetworkHintLabel.Dock = DockStyle.Fill;
        _targetContainerNetworkHintLabel.ForeColor = Color.FromArgb(96, 86, 66);
        _targetContainerNetworkHintLabel.Text = "Select a container in Target Containers. The change updates its dedicated compose file and reapplies the container on Debian.";

        AddField(table, 0, "Mode", _targetContainerNetworkModeComboBox);
        AddField(table, 1, "Network", networkPickerPanel);
        AddField(table, 2, "IPv4", _targetContainerNetworkIpv4TextBox);
        AddField(table, 3, "IPv6", _targetContainerNetworkIpv6TextBox);
        table.Controls.Add(_applyTargetContainerNetworkButton, 1, 4);
        table.Controls.Add(_targetContainerNetworkHintLabel, 0, 5);
        table.SetColumnSpan(_targetContainerNetworkHintLabel, 2);

        group.Controls.Add(table);
        return group;
    }

    private Control BuildLogPanel()
    {
        var group = new GroupBox();
        group.Text = "Process Log";
        group.Dock = DockStyle.Fill;

        _logTextBox.Dock = DockStyle.Fill;
        _logTextBox.ReadOnly = true;
        _logTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _logTextBox.BorderStyle = BorderStyle.None;
        group.Controls.Add(_logTextBox);
        return group;
    }

    private void AddField(TableLayoutPanel table, int rowIndex, string labelText, Control control)
    {
        table.Controls.Add(BuildLabel(labelText), 0, rowIndex);
        control.Dock = DockStyle.Fill;
        table.Controls.Add(control, 1, rowIndex);
    }

    private static void ConfigureInitialSplitterDistance(SplitContainer split, int preferredDistance, int preferredPanel2Size)
    {
        var initialized = false;
        split.Layout += delegate
        {
            if (initialized)
            {
                return;
            }

            var available = split.Orientation == Orientation.Vertical
                ? split.ClientSize.Width
                : split.ClientSize.Height;

            if (available <= 0)
            {
                return;
            }

            var minDistance = split.Panel1MinSize;
            var maxDistance = available - split.Panel2MinSize - split.SplitterWidth;
            if (maxDistance < minDistance)
            {
                return;
            }

            var distance = preferredDistance > 0
                ? preferredDistance
                : available - preferredPanel2Size - split.SplitterWidth;

            distance = Math.Max(minDistance, Math.Min(maxDistance, distance));
            split.SplitterDistance = distance;
            initialized = true;
        };
    }

    private void ConfigureCommandPreview(TextBox textBox)
    {
        textBox.Dock = DockStyle.Fill;
        textBox.Multiline = true;
        textBox.ReadOnly = true;
        textBox.ScrollBars = ScrollBars.Vertical;
        textBox.BorderStyle = BorderStyle.FixedSingle;
        textBox.Font = new Font(FontFamily.GenericMonospace, 8.75F);
    }

    private Label BuildLabel(string text)
    {
        return new Label
        {
            Text = text,
            Dock = DockStyle.Fill,
            TextAlign = ContentAlignment.MiddleLeft
        };
    }

    private void ApplyTheme()
    {
        _listFontRegular = new Font("Segoe UI", 9.5F, FontStyle.Regular, GraphicsUnit.Point);
        _listFontBold = new Font("Segoe UI Semibold", 9.5F, FontStyle.Bold, GraphicsUnit.Point);

        ApplySurfaceTheme(this);
        ApplyButtonTheme(_virtualMachinesScreenButton, Color.FromArgb(224, 238, 241), AccentDarkColor);
        ApplyButtonTheme(_loadContainersButton, AccentDarkColor, Color.White);
        ApplyButtonTheme(_startMigrationButton, AccentColor, Color.White);
        ApplyButtonTheme(_selectAllButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_selectRelatedButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_clearSelectionButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_refreshNetworksButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_copyMacvlanButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_copyIpvlanButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_createMacvlanOnDebianButton, AccentColor, Color.White);
        ApplyButtonTheme(_createIpvlanOnDebianButton, AccentColor, Color.White);
        ApplyButtonTheme(_createAndUseMacvlanOnDebianButton, AccentDarkColor, Color.White);
        ApplyButtonTheme(_createAndUseIpvlanOnDebianButton, AccentDarkColor, Color.White);
        ApplyButtonTheme(_useCreatedNetworkButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_refreshNetworkInventoryButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_deleteNetworkButton, Color.FromArgb(174, 76, 60), Color.White);
        ApplyButtonTheme(_useSelectedNetworkButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_loadTargetContainersButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_startTargetContainerButton, AccentColor, Color.White);
        ApplyButtonTheme(_stopTargetContainerButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_deleteTargetContainerButton, Color.FromArgb(174, 76, 60), Color.White);
        ApplyButtonTheme(_refreshTargetContainerNetworksButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_applyTargetContainerNetworkButton, AccentDarkColor, Color.White);

        _containerListView.BackColor = SurfaceColor;
        _containerListView.ForeColor = TextColor;
        _networkModeComboBox.BackColor = SurfaceColor;
        _networkModeComboBox.ForeColor = TextColor;
        _networkNameComboBox.BackColor = SurfaceColor;
        _networkNameComboBox.ForeColor = TextColor;
        _networkIpv4TextBox.BackColor = SurfaceColor;
        _networkIpv4TextBox.ForeColor = TextColor;
        _networkIpv6TextBox.BackColor = SurfaceColor;
        _networkIpv6TextBox.ForeColor = TextColor;
        _createNetworkNameTextBox.BackColor = SurfaceColor;
        _createNetworkNameTextBox.ForeColor = TextColor;
        _createParentInterfaceTextBox.BackColor = SurfaceColor;
        _createParentInterfaceTextBox.ForeColor = TextColor;
        _createSubnetTextBox.BackColor = SurfaceColor;
        _createSubnetTextBox.ForeColor = TextColor;
        _createGatewayTextBox.BackColor = SurfaceColor;
        _createGatewayTextBox.ForeColor = TextColor;
        _createIpRangeTextBox.BackColor = SurfaceColor;
        _createIpRangeTextBox.ForeColor = TextColor;
        _createIpvlanModeComboBox.BackColor = SurfaceColor;
        _createIpvlanModeComboBox.ForeColor = TextColor;
        _macvlanCommandTextBox.BackColor = Color.FromArgb(252, 249, 244);
        _macvlanCommandTextBox.ForeColor = TextColor;
        _ipvlanCommandTextBox.BackColor = Color.FromArgb(252, 249, 244);
        _ipvlanCommandTextBox.ForeColor = TextColor;
        _networkInventoryListView.BackColor = SurfaceColor;
        _networkInventoryListView.ForeColor = TextColor;
        _networkInventoryDetailsTextBox.BackColor = Color.FromArgb(252, 249, 244);
        _networkInventoryDetailsTextBox.ForeColor = TextColor;
        _targetContainerListView.BackColor = SurfaceColor;
        _targetContainerListView.ForeColor = TextColor;
        _targetContainerNetworkModeComboBox.BackColor = SurfaceColor;
        _targetContainerNetworkModeComboBox.ForeColor = TextColor;
        _targetContainerNetworkNameComboBox.BackColor = SurfaceColor;
        _targetContainerNetworkNameComboBox.ForeColor = TextColor;
        _targetContainerNetworkIpv4TextBox.BackColor = SurfaceColor;
        _targetContainerNetworkIpv4TextBox.ForeColor = TextColor;
        _targetContainerNetworkIpv6TextBox.BackColor = SurfaceColor;
        _targetContainerNetworkIpv6TextBox.ForeColor = TextColor;
        _targetContainerDetailsTextBox.BackColor = Color.FromArgb(252, 249, 244);
        _targetContainerDetailsTextBox.ForeColor = TextColor;
        _detailsTextBox.BackColor = SurfaceColor;
        _detailsTextBox.ForeColor = TextColor;
        _logTextBox.BackColor = Color.FromArgb(252, 249, 244);
        _logTextBox.ForeColor = TextColor;
        _progressBar.Style = ProgressBarStyle.Continuous;
    }

    private void ApplySurfaceTheme(Control parent)
    {
        if (Equals(parent.Tag, "header"))
        {
            return;
        }

        foreach (Control control in parent.Controls)
        {
            if (Equals(control.Tag, "header"))
            {
                continue;
            }

            if (control is GroupBox || control is Panel || control is SplitContainer)
            {
                control.BackColor = SurfaceColor;
                control.ForeColor = TextColor;
            }
            else if (control is TableLayoutPanel || control is FlowLayoutPanel)
            {
                control.BackColor = Color.Transparent;
                control.ForeColor = TextColor;
            }
            else if (control is TextBox || control is RichTextBox || control is ListView || control is ComboBox)
            {
                control.BackColor = SurfaceColor;
                control.ForeColor = TextColor;
            }
            else
            {
                control.ForeColor = TextColor;
            }

            ApplySurfaceTheme(control);
        }
    }

    private void ApplyButtonTheme(Button button, Color backColor, Color foreColor)
    {
        button.AutoSize = true;
        button.Padding = new Padding(14, 6, 14, 6);
        button.FlatStyle = FlatStyle.Flat;
        button.FlatAppearance.BorderSize = 0;
        button.BackColor = backColor;
        button.ForeColor = foreColor;
        button.Font = new Font("Segoe UI Semibold", 9.5F, FontStyle.Bold, GraphicsUnit.Point);
        button.Margin = new Padding(0, 0, 10, 0);
        button.Cursor = Cursors.Hand;
        button.TextAlign = ContentAlignment.MiddleCenter;
        button.ImageAlign = ContentAlignment.MiddleCenter;
    }

    private void InitializeDefaults()
    {
        _sourcePortTextBox.Text = "22";
        _targetPortTextBox.Text = "22";
        _targetRootTextBox.Text = MigratorCore.DefaultTargetRoot;
        _stopSourceContainersCheckBox.Checked = false;
        _dryRunCheckBox.Checked = false;
        _createIpvlanModeComboBox.SelectedItem = "l2";
        _targetContainerNetworkModeComboBox.SelectedItem = DhcpNetworkModeLabel;
        _statusLabel.Text = "Ready.";
        UpdateRunButtonText();
        UpdateNetworkCreateCommandPreview();
        UpdateTargetNetworkButtonState();
        LoadNetworkEditor(null);
        RefreshSelectedContainerDetails();
    }

    private void InitializeLogger()
    {
        var baseDir = AppDomain.CurrentDomain.BaseDirectory;
        var logsDir = Path.Combine(baseDir, "logs");
        var fileName = "gui-session-" + DateTime.Now.ToString("yyyyMMdd-HHmmss") + ".log";
        _logger = new SessionLogger(Path.Combine(logsDir, fileName));
        _logPathLabel.Text = _logger.Path;
        AppendLogLine("Session started. Version: " + Program.AppVersion);
    }

    private static string BuildAppTitle()
    {
        return "Docker Synology Migrator v" + Program.AppVersion;
    }

    private void MainFormFormClosing(object sender, FormClosingEventArgs e)
    {
        SaveConnectionProfile();
    }

    private void MainFormShown(object sender, EventArgs e)
    {
        ApplySavedLayoutProfile();
    }

    private void LoadSavedProfile()
    {
        if (!File.Exists(_profilePath))
        {
            return;
        }

        try
        {
            var json = File.ReadAllText(_profilePath, Encoding.UTF8);
            var profile = Json.Deserialize<ConnectionProfile>(json);
            if (profile == null)
            {
                return;
            }

            ApplySavedText(_sourceHostTextBox, profile.SourceHost);
            ApplySavedText(_sourcePortTextBox, profile.SourcePort);
            ApplySavedText(_sourceUserTextBox, profile.SourceLogin);
            ApplySavedText(_targetHostTextBox, profile.TargetHost);
            ApplySavedText(_targetPortTextBox, profile.TargetPort);
            ApplySavedText(_targetUserTextBox, profile.TargetLogin);
            _savePasswordsCheckBox.Checked = profile.SavePasswords;
            if (profile.SavePasswords)
            {
                ApplySavedText(_sourcePasswordTextBox, profile.SourcePassword);
                ApplySavedText(_targetPasswordTextBox, profile.TargetPassword);
            }
            ApplySavedText(_targetRootTextBox, profile.TargetRoot);
            ApplySavedText(_createNetworkNameTextBox, profile.CreatorNetworkName);
            ApplySavedText(_createParentInterfaceTextBox, profile.CreatorParentInterface);
            ApplySavedText(_createSubnetTextBox, profile.CreatorSubnet);
            ApplySavedText(_createGatewayTextBox, profile.CreatorGateway);
            ApplySavedText(_createIpRangeTextBox, profile.CreatorIpRange);
            if (!string.IsNullOrWhiteSpace(profile.CreatorIpvlanMode))
            {
                _createIpvlanModeComboBox.SelectedItem = profile.CreatorIpvlanMode.Trim();
            }

            RestoreWindowLayout(profile);
            _pendingLayoutProfile = profile;
            UpdateNetworkCreateCommandPreview();
            UpdateTargetNetworkButtonState();
            AppendLogLine("Saved connection profile loaded.");
        }
        catch (Exception ex)
        {
            AppendLogLine("[!] Failed to load saved connection profile: " + ex.Message);
        }
    }

    private void SaveConnectionProfile()
    {
        try
        {
            var profile = File.Exists(_profilePath)
                ? Json.Deserialize<ConnectionProfile>(File.ReadAllText(_profilePath, Encoding.UTF8))
                : null;
            if (profile == null)
            {
                profile = new ConnectionProfile();
            }

            profile.SourceHost = NormalizeSavedValue(_sourceHostTextBox.Text);
            profile.SourcePort = NormalizeSavedValue(_sourcePortTextBox.Text);
            profile.SourceLogin = NormalizeSavedValue(_sourceUserTextBox.Text);
            profile.TargetHost = NormalizeSavedValue(_targetHostTextBox.Text);
            profile.TargetPort = NormalizeSavedValue(_targetPortTextBox.Text);
            profile.TargetLogin = NormalizeSavedValue(_targetUserTextBox.Text);
            profile.SavePasswords = _savePasswordsCheckBox.Checked;
            profile.SourcePassword = _savePasswordsCheckBox.Checked ? NormalizeSavedValue(_sourcePasswordTextBox.Text) : null;
            profile.TargetPassword = _savePasswordsCheckBox.Checked ? NormalizeSavedValue(_targetPasswordTextBox.Text) : null;
            profile.TargetRoot = NormalizeSavedValue(_targetRootTextBox.Text) ?? MigratorCore.DefaultTargetRoot;
            profile.CreatorNetworkName = NormalizeSavedValue(_createNetworkNameTextBox.Text);
            profile.CreatorParentInterface = NormalizeSavedValue(_createParentInterfaceTextBox.Text);
            profile.CreatorSubnet = NormalizeSavedValue(_createSubnetTextBox.Text);
            profile.CreatorGateway = NormalizeSavedValue(_createGatewayTextBox.Text);
            profile.CreatorIpRange = NormalizeSavedValue(_createIpRangeTextBox.Text);
            profile.CreatorIpvlanMode = NormalizeSavedValue((_createIpvlanModeComboBox.SelectedItem ?? string.Empty).ToString());

            var restoreBounds = WindowState == FormWindowState.Normal ? Bounds : RestoreBounds;
            profile.WindowLeft = restoreBounds.Left;
            profile.WindowTop = restoreBounds.Top;
            profile.WindowWidth = restoreBounds.Width;
            profile.WindowHeight = restoreBounds.Height;
            profile.WindowState = WindowState == FormWindowState.Maximized ? "Maximized" : "Normal";
            profile.WorkspaceSplitterDistance = CaptureSplitterDistance(_workspaceSplitContainer);
            profile.MainSplitterDistance = CaptureSplitterDistance(_mainSplitContainer);
            profile.TargetContainerSplitterDistance = CaptureSplitterDistance(_targetContainerSplitContainer);
            profile.NetworkInventorySplitterDistance = CaptureSplitterDistance(_networkInventorySplitContainer);
            profile.SourceContainerColumnWidths = CaptureColumnWidths(_containerListView);
            profile.TargetContainerColumnWidths = CaptureColumnWidths(_targetContainerListView);
            profile.NetworkInventoryColumnWidths = CaptureColumnWidths(_networkInventoryListView);

            var directory = Path.GetDirectoryName(_profilePath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(_profilePath, Json.Serialize(profile), Encoding.UTF8);
        }
        catch
        {
        }
    }

    private static void ApplySavedText(TextBox textBox, string value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            textBox.Text = value.Trim();
        }
    }

    private static string NormalizeSavedValue(string value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private void RestoreWindowLayout(ConnectionProfile profile)
    {
        if (profile == null || !profile.WindowWidth.HasValue || !profile.WindowHeight.HasValue)
        {
            return;
        }

        var bounds = new Rectangle(
            profile.WindowLeft ?? Left,
            profile.WindowTop ?? Top,
            profile.WindowWidth.Value,
            profile.WindowHeight.Value);

        if (!IsUsableWindowBounds(bounds))
        {
            return;
        }

        StartPosition = FormStartPosition.Manual;
        DesktopBounds = bounds;
        if (string.Equals(profile.WindowState, "Maximized", StringComparison.OrdinalIgnoreCase))
        {
            WindowState = FormWindowState.Maximized;
        }
    }

    private void ApplySavedLayoutProfile()
    {
        if (_savedLayoutApplied)
        {
            return;
        }

        _savedLayoutApplied = true;
        var profile = _pendingLayoutProfile;
        if (profile == null)
        {
            return;
        }

        BeginInvoke((Action)delegate
        {
            ApplyColumnWidths(_containerListView, profile.SourceContainerColumnWidths);
            ApplyColumnWidths(_targetContainerListView, profile.TargetContainerColumnWidths);
            ApplyColumnWidths(_networkInventoryListView, profile.NetworkInventoryColumnWidths);
            ApplySplitterDistance(_workspaceSplitContainer, profile.WorkspaceSplitterDistance);
            ApplySplitterDistance(_mainSplitContainer, profile.MainSplitterDistance);
            ApplySplitterDistance(_targetContainerSplitContainer, profile.TargetContainerSplitterDistance);
            ApplySplitterDistance(_networkInventorySplitContainer, profile.NetworkInventorySplitterDistance);
        });
    }

    private static bool IsUsableWindowBounds(Rectangle bounds)
    {
        if (bounds.Width < 1240 || bounds.Height < 820)
        {
            return false;
        }

        return Screen.AllScreens.Any(screen => screen.WorkingArea.IntersectsWith(bounds));
    }

    private static int? CaptureSplitterDistance(SplitContainer split)
    {
        try
        {
            return split.SplitterDistance > 0 ? (int?)split.SplitterDistance : null;
        }
        catch
        {
            return null;
        }
    }

    private static void ApplySplitterDistance(SplitContainer split, int? distance)
    {
        if (!distance.HasValue || distance.Value <= 0)
        {
            return;
        }

        var available = split.Orientation == Orientation.Vertical
            ? split.ClientSize.Width
            : split.ClientSize.Height;

        if (available <= 0)
        {
            return;
        }

        var minDistance = split.Panel1MinSize;
        var maxDistance = available - split.Panel2MinSize - split.SplitterWidth;
        if (maxDistance < minDistance)
        {
            return;
        }

        split.SplitterDistance = Math.Max(minDistance, Math.Min(maxDistance, distance.Value));
    }

    private static int[] CaptureColumnWidths(ListView listView)
    {
        var widths = new int[listView.Columns.Count];
        for (var i = 0; i < listView.Columns.Count; i++)
        {
            widths[i] = listView.Columns[i].Width;
        }

        return widths;
    }

    private static void ApplyColumnWidths(ListView listView, int[] widths)
    {
        if (widths == null || widths.Length == 0)
        {
            return;
        }

        var count = Math.Min(listView.Columns.Count, widths.Length);
        for (var i = 0; i < count; i++)
        {
            if (widths[i] >= 32)
            {
                listView.Columns[i].Width = widths[i];
            }
        }
    }

    private void TargetConnectionFieldsChanged(object sender, EventArgs e)
    {
        UpdateTargetNetworkButtonState();
    }

    private void OpenVirtualMachinesScreenButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        SaveConnectionProfile();
        var vmForm = new VmMigrationForm(this);
        Hide();
        vmForm.Show();
    }

    private void UpdateTargetNetworkButtonState()
    {
        var canLoadTargetDocker = !_busy && CanLoadTargetNetworks();
        var selectedTargetContainer = GetSelectedTargetContainerDefinition();

        _refreshNetworksButton.Enabled = canLoadTargetDocker;
        _refreshNetworkInventoryButton.Enabled = canLoadTargetDocker;
        _copyMacvlanButton.Enabled = CanCreateDockerNetwork("macvlan");
        _copyIpvlanButton.Enabled = CanCreateDockerNetwork("ipvlan");
        _createMacvlanOnDebianButton.Enabled = canLoadTargetDocker && CanCreateDockerNetwork("macvlan");
        _createIpvlanOnDebianButton.Enabled = canLoadTargetDocker && CanCreateDockerNetwork("ipvlan");
        _createAndUseMacvlanOnDebianButton.Enabled = canLoadTargetDocker && GetSelectedContainer() != null && CanCreateDockerNetwork("macvlan");
        _createAndUseIpvlanOnDebianButton.Enabled = canLoadTargetDocker && GetSelectedContainer() != null && CanCreateDockerNetwork("ipvlan");
        _deleteNetworkButton.Enabled = canLoadTargetDocker && CanDeleteSelectedNetwork();
        _useSelectedNetworkButton.Enabled = !_busy && CanUseSelectedNetwork();
        _useCreatedNetworkButton.Enabled = !_busy && !string.IsNullOrWhiteSpace(NormalizeSavedValue(_createNetworkNameTextBox.Text));
        _networkInventoryListView.Enabled = !_busy;
        _loadTargetContainersButton.Enabled = canLoadTargetDocker;
        _targetContainerListView.Enabled = !_busy;
        _startTargetContainerButton.Enabled = canLoadTargetDocker && selectedTargetContainer != null && !selectedTargetContainer.Running;
        _stopTargetContainerButton.Enabled = canLoadTargetDocker && selectedTargetContainer != null && selectedTargetContainer.Running;
        _deleteTargetContainerButton.Enabled = canLoadTargetDocker && selectedTargetContainer != null;
        _refreshTargetContainerNetworksButton.Enabled = canLoadTargetDocker;
        _targetContainerNetworkModeComboBox.Enabled = !_busy && selectedTargetContainer != null;
        _targetContainerNetworkNameComboBox.Enabled = !_busy && selectedTargetContainer != null;
        _targetContainerNetworkIpv4TextBox.Enabled = !_busy && selectedTargetContainer != null;
        _targetContainerNetworkIpv6TextBox.Enabled = !_busy && selectedTargetContainer != null;
        _applyTargetContainerNetworkButton.Enabled = canLoadTargetDocker && selectedTargetContainer != null;
    }

    private bool CanLoadTargetNetworks()
    {
        ConnectionInfoData target;
        return TryBuildTargetConnectionInfo(out target);
    }

    private bool CanCreateDockerNetwork(string driver)
    {
        DockerNetworkCreateRequest request;
        string errorMessage;
        return TryBuildDockerNetworkRequest(driver, out request, out errorMessage);
    }

    private bool CanDeleteSelectedNetwork()
    {
        var definition = GetSelectedTargetNetworkDefinition();
        return definition != null && IsUserDefinedNetwork(definition.Name);
    }

    private bool CanUseSelectedNetwork()
    {
        var definition = GetSelectedTargetNetworkDefinition();
        if (definition == null)
        {
            return false;
        }

        return IsUserDefinedNetwork(definition.Name) ||
               string.Equals(definition.Name, "host", StringComparison.OrdinalIgnoreCase);
    }

    private void NetworkCreateFieldsChanged(object sender, EventArgs e)
    {
        UpdateNetworkCreateCommandPreview();
        UpdateTargetNetworkButtonState();
    }

    private void UpdateNetworkCreateCommandPreview()
    {
        _macvlanCommandTextBox.Text = BuildNetworkCreateCommand("macvlan");
        _ipvlanCommandTextBox.Text = BuildNetworkCreateCommand("ipvlan");
    }

    private string BuildNetworkCreateCommand(string driver)
    {
        if (string.IsNullOrWhiteSpace(NormalizeSavedValue(_createNetworkNameTextBox.Text)) ||
            string.IsNullOrWhiteSpace(NormalizeSavedValue(_createParentInterfaceTextBox.Text)) ||
            string.IsNullOrWhiteSpace(NormalizeSavedValue(_createSubnetTextBox.Text)))
        {
            return CreatorDefaultsHint;
        }

        try
        {
            return MigratorCore.PreviewDockerNetworkCreateCommand(BuildDockerNetworkRequest(driver));
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private DockerNetworkCreateRequest BuildDockerNetworkRequest(string driver)
    {
        return new DockerNetworkCreateRequest
        {
            Driver = driver,
            Name = NormalizeSavedValue(_createNetworkNameTextBox.Text),
            ParentInterface = NormalizeSavedValue(_createParentInterfaceTextBox.Text),
            Subnet = NormalizeSavedValue(_createSubnetTextBox.Text),
            Gateway = NormalizeSavedValue(_createGatewayTextBox.Text),
            IpRange = NormalizeSavedValue(_createIpRangeTextBox.Text),
            IpvlanMode = NormalizeSavedValue((_createIpvlanModeComboBox.SelectedItem ?? string.Empty).ToString()) ?? "l2"
        };
    }

    private bool TryBuildDockerNetworkRequest(string driver, out DockerNetworkCreateRequest request, out string errorMessage)
    {
        request = BuildDockerNetworkRequest(driver);
        errorMessage = null;

        try
        {
            MigratorCore.PreviewDockerNetworkCreateCommand(request);
            return true;
        }
        catch (Exception ex)
        {
            errorMessage = ex.Message;
            return false;
        }
    }

    private void CopyMacvlanButtonClick(object sender, EventArgs e)
    {
        CopyCommandToClipboard("macvlan");
    }

    private void CopyIpvlanButtonClick(object sender, EventArgs e)
    {
        CopyCommandToClipboard("ipvlan");
    }

    private void CopyCommandToClipboard(string driver)
    {
        DockerNetworkCreateRequest request;
        string errorMessage;
        if (!TryBuildDockerNetworkRequest(driver, out request, out errorMessage))
        {
            ShowError(errorMessage ?? "Fill in network name, parent, and subnet first.");
            return;
        }

        try
        {
            var commandText = MigratorCore.PreviewDockerNetworkCreateCommand(request);
            Clipboard.SetText(commandText);
            AppendLogLine(driver + " command copied to clipboard.");
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
        }
    }

    private void UseCreatedNetworkButtonClick(object sender, EventArgs e)
    {
        var networkName = NormalizeSavedValue(_createNetworkNameTextBox.Text);
        if (string.IsNullOrWhiteSpace(networkName))
        {
            ShowError("Enter a network name to apply it to migration.");
            return;
        }

        ApplyTargetNetworkToSelectedContainer(networkName, false);
    }

    private void RefreshTargetNetworksButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To load Debian networks, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        RefreshTargetNetworkInventory(target, "Loading Debian Docker networks...");
    }

    private void LoadContainersButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        ConnectionInfoData source;
        try
        {
            source = BuildConnectionInfo(_sourceHostTextBox, _sourcePortTextBox, _sourceUserTextBox, _sourcePasswordTextBox, "Synology");
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
            return;
        }

        var targetRoot = NormalizeTargetRoot();
        SaveConnectionProfile();
        List<ContainerDefinition> definitions = null;
        List<DockerNetworkDefinition> targetNetworks = null;
        List<ContainerDefinition> targetContainers = null;
        string targetNetworkError = null;
        string targetContainerError = null;

        RunWorker(
            "Loading containers from Synology...",
            delegate
            {
                definitions = MigratorCore.DiscoverContainers(source, targetRoot);
                ConnectionInfoData target;
                if (TryBuildTargetConnectionInfo(out target))
                {
                    try
                    {
                        targetNetworks = MigratorCore.DiscoverDockerNetworkDefinitions(target);
                    }
                    catch (Exception ex)
                    {
                        targetNetworkError = ex.Message;
                    }

                    try
                    {
                        targetContainers = MigratorCore.DiscoverTargetContainers(target, targetRoot);
                    }
                    catch (Exception ex)
                    {
                        targetContainerError = ex.Message;
                    }
                }
            },
            delegate
            {
                _containers.Clear();
                _containers.AddRange(ContainerRelationshipBuilder.Build(definitions));
                if (targetNetworks != null)
                {
                    UpdateTargetNetworkInventory(targetNetworks);
                    AppendLogLine("Loaded Debian Docker networks: " + (targetNetworks.Count == 0 ? "none" : string.Join(", ", targetNetworks.Select(item => item.Name))));
                }
                else if (!string.IsNullOrWhiteSpace(targetNetworkError))
                {
                    AppendLogLine("[!] Debian Docker networks were not loaded automatically: " + targetNetworkError);
                }

                if (targetContainers != null)
                {
                    UpdateTargetContainerInventory(targetContainers);
                    AppendLogLine("Loaded Debian target containers: " + (targetContainers.Count == 0 ? "none" : string.Join(", ", targetContainers.Select(item => item.Name))));
                }
                else if (!string.IsNullOrWhiteSpace(targetContainerError))
                {
                    AppendLogLine("[!] Debian target containers were not loaded automatically: " + targetContainerError);
                }
                PopulateContainerList();
                AppendLogLine("Loaded " + _containers.Count + " container(s).");
            });
    }

    private void DryRunCheckBoxChanged(object sender, EventArgs e)
    {
        UpdateRunButtonText();
        if (_dryRunCheckBox.Checked)
        {
            _statusLabel.Text = "Dry Run enabled. The app will validate and build a plan without copying data.";
        }
        else if (!_busy)
        {
            _statusLabel.Text = "Ready.";
        }
    }

    private void StartMigrationButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        PersistCurrentNetworkEditor();

        if (_containers.Count == 0)
        {
            ShowError("Load containers from Synology first.");
            return;
        }

        var selected = GetCheckedContainers().ToList();
        if (selected.Count == 0)
        {
            ShowError("Select at least one container for migration.");
            return;
        }

        ConnectionInfoData source;
        ConnectionInfoData target;
        try
        {
            source = BuildConnectionInfo(_sourceHostTextBox, _sourcePortTextBox, _sourceUserTextBox, _sourcePasswordTextBox, "Synology");
            target = BuildConnectionInfo(_targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox, "Debian");
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
            return;
        }

        var missingRelated = GetMissingRelatedContainers(selected);
        if (missingRelated.Count > 0)
        {
            var message = "Some selected containers are related to containers that are not selected:" + Environment.NewLine + Environment.NewLine +
                          string.Join(Environment.NewLine, missingRelated.Select(item => " - " + item)) + Environment.NewLine + Environment.NewLine +
                          "Yes - add related containers and continue." + Environment.NewLine +
                          "No - continue with current selection." + Environment.NewLine +
                          "Cancel - stop.";
            var decision = MessageBox.Show(this, message, "Related Containers", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Warning);
            if (decision == DialogResult.Cancel)
            {
                return;
            }

            if (decision == DialogResult.Yes)
            {
                foreach (var name in missingRelated)
                {
                    SetCheckedState(name, true);
                }

                selected = GetCheckedContainers().ToList();
            }
        }

        List<ContainerNetworkOverride> networkOverrides;
        try
        {
            networkOverrides = BuildNetworkOverrides(selected);
            foreach (var networkOverride in networkOverrides.OrderBy(item => item.ContainerName, StringComparer.OrdinalIgnoreCase))
            {
                AppendLogLine("[*] Network override " + networkOverride.ContainerName + ": mode=" +
                              (networkOverride.Mode ?? NetworkOverrideModes.Source) +
                              "; network=" + (networkOverride.NetworkName ?? "-") +
                              "; ipv4=" + (networkOverride.IPv4Address ?? "-") +
                              "; ipv6=" + (networkOverride.IPv6Address ?? "-"));
            }
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
            return;
        }

        var dhcpIpWarnings = FindDhcpIpReferenceWarnings(selected, networkOverrides);
        if (dhcpIpWarnings.Count > 0)
        {
            AppendLogLine("[!] Detected hardcoded IP references to containers that are being migrated in DHCP mode.");
            foreach (var warning in dhcpIpWarnings.OrderBy(item => item.ReferencingContainerName, StringComparer.OrdinalIgnoreCase)
                                                 .ThenBy(item => item.ReferencedContainerName, StringComparer.OrdinalIgnoreCase)
                                                 .ThenBy(item => item.FieldName, StringComparer.OrdinalIgnoreCase))
            {
                AppendLogLine("[!] " + BuildDhcpIpWarningText(warning));
            }

            if (!ConfirmDhcpIpWarnings(dhcpIpWarnings))
            {
                return;
            }
        }

        var targetRoot = NormalizeTargetRoot();
        SaveConnectionProfile();
        if (_dryRunCheckBox.Checked)
        {
            ResetMigrationProgress();
        }
        else
        {
            InitializeMigrationProgress(selected);
        }
        var options = new MigrationOptions
        {
            Source = source,
            Target = target,
            ContainerNames = selected.Select(item => item.Definition.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
            NetworkOverrides = networkOverrides,
            TargetRoot = targetRoot,
            StopContainersDuringBackup = _stopSourceContainersCheckBox.Checked,
            DryRun = _dryRunCheckBox.Checked
        };
        List<ContainerDefinition> refreshedTargetContainers = null;

        RunWorker(
            _dryRunCheckBox.Checked ? "Building dry run plan..." : "Migrating selected containers...",
            delegate
            {
                MigratorCore.RunMigration(options);
                if (!options.DryRun)
                {
                    refreshedTargetContainers = MigratorCore.DiscoverTargetContainers(target, targetRoot);
                }
            },
            delegate
            {
                if (_dryRunCheckBox.Checked)
                {
                    AppendLogLine("Dry run plan completed. No data was copied and no changes were applied.");
                    MessageBox.Show(this, "Dry Run plan has been built. Review the process log for the execution plan.", "Dry Run Ready", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                else
                {
                    if (refreshedTargetContainers != null)
                    {
                        UpdateTargetContainerInventory(refreshedTargetContainers);
                    }
                    AppendLogLine("Migration completed. Compose files directory on target: " + targetRoot.TrimEnd('/') + "/compose.d");
                    MessageBox.Show(this, "Migration completed successfully.", "Done", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
            });
    }

    private List<ContainerNetworkOverride> BuildNetworkOverrides(List<ContainerViewModel> selected)
    {
        var overrides = new List<ContainerNetworkOverride>();
        var activeEditorName = NormalizeOptionalValue(_activeNetworkEditorContainerName);
        var shouldUseEditorForSingleSelection = selected.Count == 1 && _networkModeComboBox.Enabled;
        foreach (var model in selected.OrderBy(item => item.Definition.Name, StringComparer.OrdinalIgnoreCase))
        {
            var useEditorValues =
                (activeEditorName != null && string.Equals(model.Definition.Name, activeEditorName, StringComparison.OrdinalIgnoreCase)) ||
                shouldUseEditorForSingleSelection;

            var networkOverride = useEditorValues
                ? BuildNetworkOverrideFromEditor(model)
                : CloneNetworkOverride(model);

            ValidateNetworkOverride(model, networkOverride);
            overrides.Add(networkOverride);
        }

        return overrides;
    }

    private List<DhcpIpReferenceWarning> FindDhcpIpReferenceWarnings(List<ContainerViewModel> selected, List<ContainerNetworkOverride> networkOverrides)
    {
        var warnings = new List<DhcpIpReferenceWarning>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var overrideMap = (networkOverrides ?? new List<ContainerNetworkOverride>())
            .Where(item => item != null && !string.IsNullOrWhiteSpace(item.ContainerName))
            .GroupBy(item => item.ContainerName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Last(), StringComparer.OrdinalIgnoreCase);

        var dhcpTargets = selected
            .Where(model => model != null && model.Definition != null)
            .Select(model => new
            {
                Model = model,
                Mode = NormalizeNetworkOverrideMode(overrideMap.ContainsKey(model.Definition.Name) ? overrideMap[model.Definition.Name].Mode : null),
                Addresses = GetDefinitionIpAddresses(model.Definition).ToList()
            })
            .Where(item => string.Equals(item.Mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase) && item.Addresses.Count > 0)
            .ToList();

        if (dhcpTargets.Count == 0)
        {
            return warnings;
        }

        foreach (var sourceModel in selected.Where(item => item != null && item.Definition != null))
        {
            foreach (var field in EnumerateIpReferenceFields(sourceModel.Definition))
            {
                foreach (var target in dhcpTargets)
                {
                    foreach (var address in target.Addresses)
                    {
                        if (!ContainsReferencedIp(field.Value, address))
                        {
                            continue;
                        }

                        var key = sourceModel.Definition.Name + "|" + target.Model.Definition.Name + "|" + address + "|" + field.Name;
                        if (!seen.Add(key))
                        {
                            continue;
                        }

                        warnings.Add(new DhcpIpReferenceWarning
                        {
                            ReferencingContainerName = sourceModel.Definition.Name,
                            ReferencedContainerName = target.Model.Definition.Name,
                            Address = address,
                            FieldName = field.Name,
                            Sample = field.Value
                        });
                    }
                }
            }
        }

        return warnings;
    }

    private bool ConfirmDhcpIpWarnings(List<DhcpIpReferenceWarning> warnings)
    {
        var preview = warnings
            .OrderBy(item => item.ReferencingContainerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ReferencedContainerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.FieldName, StringComparer.OrdinalIgnoreCase)
            .Take(12)
            .Select(item => " - " + BuildDhcpIpWarningText(item))
            .ToList();

        var moreCount = warnings.Count - preview.Count;
        var message = "Hardcoded IP references were detected to containers that are being migrated in DHCP mode." + Environment.NewLine + Environment.NewLine +
                      "These IP addresses are likely to change on Debian, so container-to-container communication may fail until the application configs are updated." + Environment.NewLine + Environment.NewLine +
                      "Detected references:" + Environment.NewLine +
                      string.Join(Environment.NewLine, preview);

        if (moreCount > 0)
        {
            message += Environment.NewLine + " - ... and " + moreCount + " more";
        }

        message += Environment.NewLine + Environment.NewLine +
                   "This check scans Docker metadata only: Environment, Command, Entrypoint, Extra hosts, and Labels. Files inside mounted volumes are not scanned." + Environment.NewLine + Environment.NewLine +
                   "Yes - continue anyway." + Environment.NewLine +
                   "No - stop and review the configs or switch containers to Static IP.";

        return MessageBox.Show(this, message, "DHCP IP References Detected", MessageBoxButtons.YesNo, MessageBoxIcon.Warning) == DialogResult.Yes;
    }

    private static string BuildDhcpIpWarningText(DhcpIpReferenceWarning warning)
    {
        return warning.ReferencingContainerName + " references " +
               warning.ReferencedContainerName + " at " +
               warning.Address + " in " +
               warning.FieldName + " (" + BuildWarningSample(warning.Sample) + ")";
    }

    private static string BuildWarningSample(string value)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length <= 72)
        {
            return normalized;
        }

        return normalized.Substring(0, 69) + "...";
    }

    private static IEnumerable<IpReferenceField> EnumerateIpReferenceFields(ContainerDefinition definition)
    {
        foreach (var value in definition.Environment ?? new List<string>())
        {
            yield return new IpReferenceField { Name = "Environment", Value = value };
        }

        foreach (var value in definition.Command ?? new List<string>())
        {
            yield return new IpReferenceField { Name = "Command", Value = value };
        }

        foreach (var value in definition.Entrypoint ?? new List<string>())
        {
            yield return new IpReferenceField { Name = "Entrypoint", Value = value };
        }

        foreach (var value in definition.ExtraHosts ?? new List<string>())
        {
            yield return new IpReferenceField { Name = "Extra hosts", Value = value };
        }

        foreach (var item in (definition.Labels ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase))
            .OrderBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase))
        {
            yield return new IpReferenceField { Name = "Label " + item.Key, Value = item.Value };
        }
    }

    private static IEnumerable<string> GetDefinitionIpAddresses(ContainerDefinition definition)
    {
        return (definition.NetworkAttachments ?? new List<ContainerNetworkAttachment>())
            .SelectMany(item => new[]
            {
                item == null ? null : NormalizeOptionalValue(item.IPv4Address),
                item == null ? null : NormalizeOptionalValue(item.IPv6Address)
            })
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase);
    }

    private static bool ContainsReferencedIp(string text, string address)
    {
        var normalizedText = NormalizeOptionalValue(text);
        var normalizedAddress = NormalizeOptionalValue(address);
        if (normalizedText == null || normalizedAddress == null)
        {
            return false;
        }

        if (normalizedAddress.IndexOf(':') >= 0)
        {
            return normalizedText.IndexOf(normalizedAddress, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        return Regex.IsMatch(normalizedText, @"(?<!\d)" + Regex.Escape(normalizedAddress) + @"(?!\d)");
    }

    private ContainerNetworkOverride BuildNetworkOverrideFromEditor(ContainerViewModel model)
    {
        return new ContainerNetworkOverride
        {
            ContainerName = model.Definition.Name,
            Mode = GetSelectedNetworkMode(),
            NetworkName = NormalizeOptionalValue(_networkNameComboBox.Text),
            IPv4Address = NormalizeOptionalValue(_networkIpv4TextBox.Text),
            IPv6Address = NormalizeOptionalValue(_networkIpv6TextBox.Text)
        };
    }

    private static ContainerNetworkOverride CloneNetworkOverride(ContainerViewModel model)
    {
        var source = model.NetworkOverride ?? CreateDefaultNetworkOverride(model.Definition);
        return new ContainerNetworkOverride
        {
            ContainerName = model.Definition.Name,
            Mode = NormalizeNetworkOverrideMode(source.Mode),
            NetworkName = NormalizeOptionalValue(source.NetworkName),
            IPv4Address = NormalizeOptionalValue(source.IPv4Address),
            IPv6Address = NormalizeOptionalValue(source.IPv6Address)
        };
    }

    private static void ValidateNetworkOverride(ContainerViewModel model, ContainerNetworkOverride networkOverride)
    {
        var mode = NormalizeNetworkOverrideMode(networkOverride.Mode);
        if (string.Equals(mode, NetworkOverrideModes.Source, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(mode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(networkOverride.NetworkName))
        {
            throw new InvalidOperationException("Specify a target Debian network for container " + model.Definition.Name + ".");
        }

        if (!IsUserDefinedNetwork(networkOverride.NetworkName))
        {
            throw new InvalidOperationException("Container " + model.Definition.Name + " requires a user-defined Debian Docker network in DHCP/Static IP modes. Built-in host/bridge/none networks are not valid here.");
        }

        if (string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(networkOverride.IPv4Address) && string.IsNullOrWhiteSpace(networkOverride.IPv6Address))
            {
                throw new InvalidOperationException("Specify IPv4 or IPv6 for container " + model.Definition.Name + " in Static IP mode.");
            }

            ValidateIpAddress(networkOverride.IPv4Address, model.Definition.Name, "IPv4");
            ValidateIpAddress(networkOverride.IPv6Address, model.Definition.Name, "IPv6");
        }
    }

    private static void ValidateIpAddress(string value, string containerName, string label)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        IPAddress parsed;
        if (!IPAddress.TryParse(value.Trim(), out parsed))
        {
            throw new InvalidOperationException(label + " for container " + containerName + " is invalid.");
        }
    }

    private void NetworkEditorChanged(object sender, EventArgs e)
    {
        if (_suppressNetworkEditorEvents)
        {
            return;
        }

        var model = GetSelectedContainer();
        if (model == null)
        {
            return;
        }

        SaveNetworkEditorToModel(model);
        if ((string.Equals(model.NetworkOverride.Mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase) ||
             string.Equals(model.NetworkOverride.Mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase)) &&
            !IsUserDefinedNetwork(model.NetworkOverride.NetworkName))
        {
            model.NetworkOverride.NetworkName = null;
            _networkNameComboBox.Text = string.Empty;
        }
        UpdateNetworkEditorState(model);
        RefreshSelectedContainerDetails();
    }

    private void LoadNetworkEditor(ContainerViewModel model)
    {
        _suppressNetworkEditorEvents = true;
        _activeNetworkEditorContainerName = model == null ? null : model.Definition.Name;

        if (model == null)
        {
            EnsureNetworkOverride(model);
            _networkModeComboBox.Enabled = false;
            _networkNameComboBox.Enabled = false;
            _networkIpv4TextBox.Enabled = false;
            _networkIpv6TextBox.Enabled = false;
            UpdateTargetNetworkButtonState();
            _networkModeComboBox.SelectedIndex = _networkModeComboBox.Items.Count == 0 ? -1 : 0;
            _networkNameComboBox.Text = string.Empty;
            _networkIpv4TextBox.Text = string.Empty;
            _networkIpv6TextBox.Text = string.Empty;
            _networkHintLabel.Text = "Select a container on the left. For DHCP and Static IP modes, choose a target Debian network.";
            _suppressNetworkEditorEvents = false;
            return;
        }

        EnsureNetworkOverride(model);
        _networkModeComboBox.Enabled = true;
        _networkModeComboBox.SelectedItem = GetNetworkModeLabel(model.NetworkOverride.Mode);
        if (_networkModeComboBox.SelectedIndex < 0)
        {
            _networkModeComboBox.SelectedIndex = 0;
        }

        _networkNameComboBox.Text = model.NetworkOverride.NetworkName ?? string.Empty;
        _networkIpv4TextBox.Text = model.NetworkOverride.IPv4Address ?? string.Empty;
        _networkIpv6TextBox.Text = model.NetworkOverride.IPv6Address ?? string.Empty;
        UpdateNetworkEditorState(model);
        _suppressNetworkEditorEvents = false;
    }

    private void SaveNetworkEditorToModel(ContainerViewModel model)
    {
        EnsureNetworkOverride(model);
        model.NetworkOverride.Mode = GetSelectedNetworkMode();
        model.NetworkOverride.NetworkName = NormalizeOptionalValue(_networkNameComboBox.Text);
        model.NetworkOverride.IPv4Address = NormalizeOptionalValue(_networkIpv4TextBox.Text);
        model.NetworkOverride.IPv6Address = NormalizeOptionalValue(_networkIpv6TextBox.Text);
    }

    private void PersistCurrentNetworkEditor()
    {
        if (_suppressNetworkEditorEvents || string.IsNullOrWhiteSpace(_activeNetworkEditorContainerName))
        {
            return;
        }

        var model = _containers.FirstOrDefault(item =>
            string.Equals(item.Definition.Name, _activeNetworkEditorContainerName, StringComparison.OrdinalIgnoreCase));
        if (model == null)
        {
            return;
        }

        SaveNetworkEditorToModel(model);
    }

    private void UpdateNetworkEditorState(ContainerViewModel model)
    {
        var mode = model == null ? NetworkOverrideModes.Source : GetSelectedNetworkMode();
        var hasSelection = model != null;
        var needsNetworkName = string.Equals(mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase) ||
                               string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase);
        var needsStaticIp = string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase);

        _networkModeComboBox.Enabled = hasSelection;
        _networkNameComboBox.Enabled = hasSelection && needsNetworkName;
        _networkIpv4TextBox.Enabled = hasSelection && needsStaticIp;
        _networkIpv6TextBox.Enabled = hasSelection && needsStaticIp;
        UpdateTargetNetworkButtonState();

        if (!hasSelection)
        {
            _networkHintLabel.Text = "Select a container on the left. For DHCP and Static IP modes, choose a target Debian network.";
            return;
        }

        if (string.Equals(mode, NetworkOverrideModes.Source, StringComparison.OrdinalIgnoreCase))
        {
            _networkHintLabel.Text = "The original network configuration from Synology will be preserved.";
            return;
        }

        if (string.Equals(mode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            _networkHintLabel.Text = "Host: the container will use the Debian host network. It will not have a separate IP address or Compose port publishing.";
            return;
        }

        if (string.Equals(mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase))
        {
            _networkHintLabel.Text = "DHCP: the current network layout will be replaced with a single target Debian network without a fixed IP.";
            return;
        }

        _networkHintLabel.Text = "Static IP: the current network layout will be replaced with a single target Debian network. The network must already exist and have a compatible subnet/IPAM configuration.";
    }

    private void RefreshSelectedContainerDetails()
    {
        var model = GetSelectedContainer();
        _detailsTextBox.Text = model == null ? string.Empty : GuiFormatter.BuildDetails(model);
    }

    private void LoadTargetContainersButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To load target containers, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        RefreshTargetContainerInventory(target, "Loading Debian target containers...");
    }

    private void RefreshTargetContainerInventory(ConnectionInfoData target, string busyText)
    {
        List<ContainerDefinition> definitions = null;
        var targetRoot = GetTargetInspectRoot();
        RunWorker(
            busyText,
            delegate
            {
                definitions = MigratorCore.DiscoverTargetContainers(target, targetRoot);
            },
            delegate
            {
                UpdateTargetContainerInventory(definitions);
                AppendLogLine("Loaded Debian target containers: " + (definitions.Count == 0 ? "none" : string.Join(", ", definitions.Select(item => item.Name))));
            });
    }

    private void UpdateTargetContainerInventory(List<ContainerDefinition> definitions)
    {
        var selectedName = GetSelectedTargetContainerDefinition() == null
            ? null
            : GetSelectedTargetContainerDefinition().Name;

        _targetContainerDefinitions.Clear();
        _targetContainerDefinitions.AddRange((definitions ?? new List<ContainerDefinition>())
            .Where(item => item != null)
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase));

        _targetContainerListView.BeginUpdate();
        _targetContainerListView.Items.Clear();
        foreach (var definition in _targetContainerDefinitions)
        {
            var item = new ListViewItem(definition.Name ?? string.Empty);
            item.Tag = definition;
            item.SubItems.Add(string.IsNullOrWhiteSpace(definition.Status) ? "unknown" : definition.Status);
            item.SubItems.Add(definition.Image ?? string.Empty);
            item.SubItems.Add(GuiFormatter.BuildNetworkNamesSummary(definition));
            item.SubItems.Add(GuiFormatter.BuildIpAddressSummary(definition));
            _targetContainerListView.Items.Add(item);
        }

        _targetContainerListView.EndUpdate();
        SelectTargetContainerByName(selectedName);
        if (_targetContainerListView.SelectedItems.Count == 0 && _targetContainerListView.Items.Count > 0)
        {
            _targetContainerListView.Items[0].Selected = true;
        }

        if (_targetContainerListView.SelectedItems.Count == 0)
        {
            _targetContainerDetailsTextBox.Text = "Debian target containers have not been loaded yet.";
            LoadTargetContainerNetworkEditor(null);
        }

        UpdateTargetNetworkButtonState();
    }

    private void SelectTargetContainerByName(string containerName)
    {
        if (string.IsNullOrWhiteSpace(containerName))
        {
            return;
        }

        foreach (ListViewItem item in _targetContainerListView.Items)
        {
            if (!string.Equals(item.Text, containerName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            item.Selected = true;
            item.Focused = true;
            item.EnsureVisible();
            return;
        }
    }

    private ContainerDefinition GetSelectedTargetContainerDefinition()
    {
        return _targetContainerListView.SelectedItems.Count == 0
            ? null
            : _targetContainerListView.SelectedItems[0].Tag as ContainerDefinition;
    }

    private void TargetContainerSelectionChanged(object sender, EventArgs e)
    {
        var definition = GetSelectedTargetContainerDefinition();
        _targetContainerDetailsTextBox.Text = definition == null
            ? "Select a Debian target container to view runtime details, networks, and IP addresses."
            : GuiFormatter.BuildTargetDetails(definition);
        LoadTargetContainerNetworkEditor(definition);
        UpdateTargetNetworkButtonState();
    }

    private void StartTargetContainerButtonClick(object sender, EventArgs e)
    {
        ManageTargetContainer(
            "start",
            "Starting Debian target container...",
            delegate(ConnectionInfoData target, string containerName) { MigratorCore.StartTargetContainer(target, containerName); });
    }

    private void StopTargetContainerButtonClick(object sender, EventArgs e)
    {
        ManageTargetContainer(
            "stop",
            "Stopping Debian target container...",
            delegate(ConnectionInfoData target, string containerName) { MigratorCore.StopTargetContainer(target, containerName); });
    }

    private void DeleteTargetContainerButtonClick(object sender, EventArgs e)
    {
        var definition = GetSelectedTargetContainerDefinition();
        if (definition == null)
        {
            ShowError("Select a Debian target container first.");
            return;
        }

        var decision = MessageBox.Show(
            this,
            "Delete Debian target container \"" + definition.Name + "\"?" + Environment.NewLine + Environment.NewLine +
            "This removes only the container. Volumes, bind-mounted data, and images are not deleted automatically.",
            "Delete Target Container",
            MessageBoxButtons.YesNo,
            MessageBoxIcon.Warning);

        if (decision != DialogResult.Yes)
        {
            return;
        }

        ManageTargetContainer(
            "delete",
            "Deleting Debian target container...",
            delegate(ConnectionInfoData target, string containerName) { MigratorCore.DeleteTargetContainer(target, containerName); });
    }

    private void ManageTargetContainer(string actionName, string busyText, Action<ConnectionInfoData, string> action)
    {
        if (_busy)
        {
            return;
        }

        var definition = GetSelectedTargetContainerDefinition();
        if (definition == null)
        {
            ShowError("Select a Debian target container first.");
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To manage target containers, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        var containerName = definition.Name;
        var targetRoot = GetTargetInspectRoot();
        List<ContainerDefinition> definitions = null;
        RunWorker(
            busyText,
            delegate
            {
                action(target, containerName);
                definitions = MigratorCore.DiscoverTargetContainers(target, targetRoot);
            },
            delegate
            {
                UpdateTargetContainerInventory(definitions);
                AppendLogLine("Target container action completed: " + actionName + " " + containerName);
            });
    }

    private string GetTargetInspectRoot()
    {
        return NormalizeSavedValue(_targetRootTextBox.Text) ?? MigratorCore.DefaultTargetRoot;
    }

    private void RefreshTargetNetworkInventory(ConnectionInfoData target, string busyText)
    {
        List<DockerNetworkDefinition> definitions = null;
        RunWorker(
            busyText,
            delegate
            {
                definitions = MigratorCore.DiscoverDockerNetworkDefinitions(target);
            },
            delegate
            {
                UpdateTargetNetworkInventory(definitions);
                AppendLogLine("Loaded Debian Docker networks: " + (definitions.Count == 0 ? "none" : string.Join(", ", definitions.Select(item => item.Name))));
            });
    }

    private void UpdateTargetNetworkInventory(List<DockerNetworkDefinition> definitions)
    {
        var selectedName = GetSelectedTargetNetworkDefinition() == null
            ? null
            : GetSelectedTargetNetworkDefinition().Name;

        _targetDockerNetworkDefinitions.Clear();
        _targetDockerNetworkDefinitions.AddRange((definitions ?? new List<DockerNetworkDefinition>())
            .Where(item => item != null)
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase));

        _networkInventoryListView.BeginUpdate();
        _networkInventoryListView.Items.Clear();
        foreach (var definition in _targetDockerNetworkDefinitions)
        {
            var item = new ListViewItem(definition.Name ?? string.Empty);
            item.Tag = definition;
            item.SubItems.Add(definition.Driver ?? string.Empty);
            item.SubItems.Add(definition.Scope ?? string.Empty);
            item.SubItems.Add(GuiFormatter.BuildNetworkIpamSummary(definition));
            item.SubItems.Add(definition.ParentInterface ?? string.Empty);
            if (!IsUserDefinedNetwork(definition.Name))
            {
                item.ForeColor = Color.FromArgb(122, 116, 101);
            }

            _networkInventoryListView.Items.Add(item);
        }

        _networkInventoryListView.EndUpdate();
        UpdateTargetNetworkOptions(_targetDockerNetworkDefinitions
            .Where(item => IsUserDefinedNetwork(item.Name))
            .Select(item => item.Name)
            .ToList());

        SelectTargetNetworkByName(selectedName);
        if (_networkInventoryListView.SelectedItems.Count == 0 && _networkInventoryListView.Items.Count > 0)
        {
            _networkInventoryListView.Items[0].Selected = true;
        }

        if (_networkInventoryListView.SelectedItems.Count == 0)
        {
            _networkInventoryDetailsTextBox.Text = "Debian networks have not been loaded yet.";
        }

        UpdateTargetNetworkButtonState();
    }

    private void SelectTargetNetworkByName(string networkName)
    {
        if (string.IsNullOrWhiteSpace(networkName))
        {
            return;
        }

        foreach (ListViewItem item in _networkInventoryListView.Items)
        {
            if (!string.Equals(item.Text, networkName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            item.Selected = true;
            item.Focused = true;
            item.EnsureVisible();
            return;
        }
    }

    private DockerNetworkDefinition GetSelectedTargetNetworkDefinition()
    {
        return _networkInventoryListView.SelectedItems.Count == 0
            ? null
            : _networkInventoryListView.SelectedItems[0].Tag as DockerNetworkDefinition;
    }

    private void NetworkInventorySelectionChanged(object sender, EventArgs e)
    {
        var definition = GetSelectedTargetNetworkDefinition();
        _networkInventoryDetailsTextBox.Text = definition == null
            ? "Select a Debian network to see driver, subnet, gateway, parent, and connected containers."
            : GuiFormatter.BuildDockerNetworkDetails(definition);
        UpdateTargetNetworkButtonState();
    }

    private void DeleteNetworkButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        var definition = GetSelectedTargetNetworkDefinition();
        if (definition == null)
        {
            ShowError("Select a Debian network from the list first.");
            return;
        }

        if (!IsUserDefinedNetwork(definition.Name))
        {
            ShowError("Built-in host/bridge/none networks cannot be deleted from the app.");
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To delete a Debian network, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        var decision = MessageBox.Show(
            this,
            "Delete Debian Docker network \"" + definition.Name + "\"?" + Environment.NewLine + Environment.NewLine +
            "If containers are still attached, Docker will reject the request and the network will remain.",
            "Delete Network",
            MessageBoxButtons.YesNo,
            MessageBoxIcon.Warning);

        if (decision != DialogResult.Yes)
        {
            return;
        }

        var networkName = definition.Name;
        List<DockerNetworkDefinition> definitions = null;
        RunWorker(
            "Deleting Debian Docker network...",
            delegate
            {
                MigratorCore.DeleteDockerNetwork(target, networkName);
                definitions = MigratorCore.DiscoverDockerNetworkDefinitions(target);
            },
            delegate
            {
                UpdateTargetNetworkInventory(definitions);
                AppendLogLine("Deleted Debian Docker network: " + networkName);
            });
    }

    private void UseSelectedNetworkButtonClick(object sender, EventArgs e)
    {
        var definition = GetSelectedTargetNetworkDefinition();
        if (definition == null)
        {
            ShowError("Select a Debian network from the list first.");
            return;
        }

        if (string.Equals(definition.Name, "host", StringComparison.OrdinalIgnoreCase))
        {
            ApplyTargetNetworkToSelectedContainer(null, true);
            return;
        }

        if (!IsUserDefinedNetwork(definition.Name))
        {
            ShowError("Only a user-defined Docker network or host can be applied from the list.");
            return;
        }

        ApplyTargetNetworkToSelectedContainer(definition.Name, false);
    }

    private void CreateMacvlanOnDebianButtonClick(object sender, EventArgs e)
    {
        CreateDockerNetworkOnDebian("macvlan", false);
    }

    private void CreateIpvlanOnDebianButtonClick(object sender, EventArgs e)
    {
        CreateDockerNetworkOnDebian("ipvlan", false);
    }

    private void CreateAndUseMacvlanOnDebianButtonClick(object sender, EventArgs e)
    {
        CreateDockerNetworkOnDebian("macvlan", true);
    }

    private void CreateAndUseIpvlanOnDebianButtonClick(object sender, EventArgs e)
    {
        CreateDockerNetworkOnDebian("ipvlan", true);
    }

    private void CreateDockerNetworkOnDebian(string driver, bool applyToSelectedContainer)
    {
        if (_busy)
        {
            return;
        }

        if (applyToSelectedContainer && GetSelectedContainer() == null)
        {
            ShowError("Select a container on the left first if you want the new network to be applied immediately.");
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To create a Debian network, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        DockerNetworkCreateRequest request;
        string errorMessage;
        if (!TryBuildDockerNetworkRequest(driver, out request, out errorMessage))
        {
            ShowError(errorMessage);
            return;
        }

        SaveConnectionProfile();
        List<DockerNetworkDefinition> definitions = null;
        RunWorker(
            "Creating Debian Docker network...",
            delegate
            {
                MigratorCore.CreateDockerNetwork(target, request);
                definitions = MigratorCore.DiscoverDockerNetworkDefinitions(target);
            },
            delegate
            {
                UpdateTargetNetworkInventory(definitions);
                SelectTargetNetworkByName(request.Name);
                if (applyToSelectedContainer)
                {
                    ApplyTargetNetworkToSelectedContainer(request.Name, false);
                }
                AppendLogLine("Created Debian Docker network: " + request.Name + " (" + request.Driver + ")");
            });
    }

    private void ApplyTargetNetworkToSelectedContainer(string networkName, bool useHostMode)
    {
        var selected = GetSelectedContainer();
        if (selected == null)
        {
            ShowError("Select a container on the left first, then apply a migration network.");
            return;
        }

        _suppressNetworkEditorEvents = true;
        if (useHostMode)
        {
            _networkModeComboBox.SelectedItem = HostNetworkModeLabel;
            _networkNameComboBox.Text = string.Empty;
        }
        else
        {
            var currentMode = NormalizeNetworkOverrideMode(selected.NetworkOverride == null ? null : selected.NetworkOverride.Mode);
            if (string.Equals(currentMode, NetworkOverrideModes.Source, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(currentMode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
            {
                _networkModeComboBox.SelectedItem = DhcpNetworkModeLabel;
            }

            _networkNameComboBox.Text = networkName ?? string.Empty;
        }

        _suppressNetworkEditorEvents = false;
        SaveNetworkEditorToModel(selected);
        UpdateNetworkEditorState(selected);
        RefreshSelectedContainerDetails();

        if (useHostMode)
        {
            AppendLogLine("Container " + selected.Definition.Name + " will use host network on Debian.");
        }
        else
        {
            AppendLogLine("Assigned Debian Docker network " + networkName + " to container " + selected.Definition.Name + ".");
        }
    }

    private void TargetContainerNetworkEditorChanged(object sender, EventArgs e)
    {
        if (_suppressTargetContainerNetworkEditorEvents)
        {
            return;
        }

        UpdateTargetContainerNetworkEditorState(GetSelectedTargetContainerDefinition());
    }

    private void LoadTargetContainerNetworkEditor(ContainerDefinition definition)
    {
        _suppressTargetContainerNetworkEditorEvents = true;
        try
        {
            if (definition == null)
            {
                _targetContainerNetworkModeComboBox.SelectedItem = DhcpNetworkModeLabel;
                _targetContainerNetworkNameComboBox.Text = string.Empty;
                _targetContainerNetworkIpv4TextBox.Text = string.Empty;
                _targetContainerNetworkIpv6TextBox.Text = string.Empty;
            }
            else
            {
                var current = BuildTargetContainerRuntimeOverride(definition);
                _targetContainerNetworkModeComboBox.SelectedItem = GetTargetContainerNetworkModeLabel(current.Mode);
                _targetContainerNetworkNameComboBox.Text = current.NetworkName ?? string.Empty;
                _targetContainerNetworkIpv4TextBox.Text = current.IPv4Address ?? string.Empty;
                _targetContainerNetworkIpv6TextBox.Text = current.IPv6Address ?? string.Empty;
            }
        }
        finally
        {
            _suppressTargetContainerNetworkEditorEvents = false;
        }

        UpdateTargetContainerNetworkEditorState(definition);
    }

    private void UpdateTargetContainerNetworkEditorState(ContainerDefinition definition)
    {
        var hasSelection = definition != null;
        var mode = GetSelectedTargetContainerNetworkMode();
        var needsUserDefinedNetwork = mode == NetworkOverrideModes.Dhcp || mode == NetworkOverrideModes.Static;

        _targetContainerNetworkNameComboBox.Enabled = hasSelection && needsUserDefinedNetwork && !_busy;
        _targetContainerNetworkIpv4TextBox.Enabled = hasSelection && mode == NetworkOverrideModes.Static && !_busy;
        _targetContainerNetworkIpv6TextBox.Enabled = hasSelection && mode == NetworkOverrideModes.Static && !_busy;

        if (!hasSelection)
        {
            _targetContainerNetworkHintLabel.Text = "Select a container in Target Containers to edit its persistent network settings on Debian.";
            return;
        }

        if (mode == NetworkOverrideModes.Host)
        {
            _targetContainerNetworkHintLabel.Text = "Host mode recreates the selected target container with Debian host networking.";
        }
        else if (mode == NetworkOverrideModes.Static)
        {
            _targetContainerNetworkHintLabel.Text = "Static IP mode writes the new network settings into the container's dedicated compose file and recreates only that target container.";
        }
        else
        {
            _targetContainerNetworkHintLabel.Text = "DHCP mode rewrites the selected target container to use one Debian user-defined network without fixed IP addresses.";
        }
    }

    private static string GetTargetContainerNetworkModeLabel(string mode)
    {
        if (string.Equals(mode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            return HostNetworkModeLabel;
        }

        if (string.Equals(mode, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase))
        {
            return StaticNetworkModeLabel;
        }

        return DhcpNetworkModeLabel;
    }

    private string GetSelectedTargetContainerNetworkMode()
    {
        var selected = (_targetContainerNetworkModeComboBox.SelectedItem ?? string.Empty).ToString();
        if (string.Equals(selected, HostNetworkModeLabel, StringComparison.OrdinalIgnoreCase))
        {
            return NetworkOverrideModes.Host;
        }

        if (string.Equals(selected, StaticNetworkModeLabel, StringComparison.OrdinalIgnoreCase))
        {
            return NetworkOverrideModes.Static;
        }

        return NetworkOverrideModes.Dhcp;
    }

    private static ContainerNetworkOverride BuildTargetContainerRuntimeOverride(ContainerDefinition definition)
    {
        var primaryAttachment = definition.NetworkAttachments
            .FirstOrDefault(item => item != null && IsUserDefinedNetwork(item.Name)) ??
            definition.NetworkAttachments.FirstOrDefault(item => item != null);

        if (string.Equals(definition.NetworkMode, "host", StringComparison.OrdinalIgnoreCase))
        {
            return new ContainerNetworkOverride
            {
                ContainerName = definition.Name,
                Mode = NetworkOverrideModes.Host
            };
        }

        return new ContainerNetworkOverride
        {
            ContainerName = definition.Name,
            Mode = primaryAttachment != null && primaryAttachment.HasExplicitIpamConfiguration
                ? NetworkOverrideModes.Static
                : NetworkOverrideModes.Dhcp,
            NetworkName = primaryAttachment == null ? string.Empty : primaryAttachment.Name,
            IPv4Address = primaryAttachment == null ? string.Empty : primaryAttachment.IPv4Address,
            IPv6Address = primaryAttachment == null ? string.Empty : primaryAttachment.IPv6Address
        };
    }

    private ContainerNetworkOverride BuildTargetContainerNetworkOverride(ContainerDefinition definition)
    {
        var mode = GetSelectedTargetContainerNetworkMode();
        var networkName = NormalizeSavedValue(_targetContainerNetworkNameComboBox.Text);
        var ipv4 = NormalizeSavedValue(_targetContainerNetworkIpv4TextBox.Text);
        var ipv6 = NormalizeSavedValue(_targetContainerNetworkIpv6TextBox.Text);

        if (mode == NetworkOverrideModes.Host)
        {
            return new ContainerNetworkOverride
            {
                ContainerName = definition.Name,
                Mode = NetworkOverrideModes.Host
            };
        }

        if (!IsUserDefinedNetwork(networkName))
        {
            throw new InvalidOperationException("For target container network mode " + mode + ", choose a user-defined Debian Docker network.");
        }

        if (mode == NetworkOverrideModes.Static && ipv4 == null && ipv6 == null)
        {
            throw new InvalidOperationException("Static IP mode requires IPv4 or IPv6 for the selected target container.");
        }

        return new ContainerNetworkOverride
        {
            ContainerName = definition.Name,
            Mode = mode,
            NetworkName = networkName,
            IPv4Address = ipv4,
            IPv6Address = ipv6
        };
    }

    private void ApplyTargetContainerNetworkButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        var definition = GetSelectedTargetContainerDefinition();
        if (definition == null)
        {
            ShowError("Select a Debian target container first.");
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To update a target container network, fill in Host, Port, Login, and Password in the Debian Target section.");
            return;
        }

        ContainerNetworkOverride networkOverride;
        try
        {
            networkOverride = BuildTargetContainerNetworkOverride(definition);
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
            return;
        }

        var targetRoot = GetTargetInspectRoot();
        List<ContainerDefinition> refreshedContainers = null;
        List<DockerNetworkDefinition> refreshedNetworks = null;
        RunWorker(
            "Updating target container network...",
            delegate
            {
                MigratorCore.UpdateTargetContainerNetwork(target, targetRoot, definition.Name, networkOverride);
                refreshedContainers = MigratorCore.DiscoverTargetContainers(target, targetRoot);
                refreshedNetworks = MigratorCore.DiscoverDockerNetworkDefinitions(target);
            },
            delegate
            {
                UpdateTargetContainerInventory(refreshedContainers);
                UpdateTargetNetworkInventory(refreshedNetworks);
                SelectTargetContainerByName(definition.Name);
                AppendLogLine("Updated target container network: " + definition.Name + " -> " +
                              (networkOverride.Mode == NetworkOverrideModes.Host
                                  ? "host"
                                  : networkOverride.Mode + " / " + networkOverride.NetworkName));
            });
    }

    private static void EnsureNetworkOverride(ContainerViewModel model)
    {
        if (model == null || model.NetworkOverride != null)
        {
            return;
        }

        model.NetworkOverride = CreateDefaultNetworkOverride(model.Definition);
    }

    private static ContainerNetworkOverride CreateDefaultNetworkOverride(ContainerDefinition definition)
    {
        var primaryAttachment = definition.NetworkAttachments
            .FirstOrDefault(item => item != null && IsUserDefinedNetwork(item.Name)) ??
            definition.NetworkAttachments.FirstOrDefault();

        var networkName = primaryAttachment != null
            ? primaryAttachment.Name
            : definition.Networks.FirstOrDefault() ?? string.Empty;

        return new ContainerNetworkOverride
        {
            ContainerName = definition.Name,
            Mode = NetworkOverrideModes.Source,
            NetworkName = networkName,
            IPv4Address = primaryAttachment == null ? string.Empty : primaryAttachment.IPv4Address,
            IPv6Address = primaryAttachment == null ? string.Empty : primaryAttachment.IPv6Address
        };
    }

    private string GetSelectedNetworkMode()
    {
        var selected = (_networkModeComboBox.SelectedItem ?? string.Empty).ToString();
        if (string.Equals(selected, HostNetworkModeLabel, StringComparison.OrdinalIgnoreCase))
        {
            return NetworkOverrideModes.Host;
        }

        if (string.Equals(selected, DhcpNetworkModeLabel, StringComparison.OrdinalIgnoreCase))
        {
            return NetworkOverrideModes.Dhcp;
        }

        if (string.Equals(selected, StaticNetworkModeLabel, StringComparison.OrdinalIgnoreCase))
        {
            return NetworkOverrideModes.Static;
        }

        return NetworkOverrideModes.Source;
    }

    private static string GetNetworkModeLabel(string mode)
    {
        var normalized = NormalizeNetworkOverrideMode(mode);
        if (string.Equals(normalized, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            return HostNetworkModeLabel;
        }

        if (string.Equals(normalized, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase))
        {
            return DhcpNetworkModeLabel;
        }

        if (string.Equals(normalized, NetworkOverrideModes.Static, StringComparison.OrdinalIgnoreCase))
        {
            return StaticNetworkModeLabel;
        }

        return SourceNetworkModeLabel;
    }

    private static string NormalizeNetworkOverrideMode(string mode)
    {
        var normalized = (mode ?? string.Empty).Trim().ToLowerInvariant();
        if (normalized == NetworkOverrideModes.Host ||
            normalized == NetworkOverrideModes.Dhcp ||
            normalized == NetworkOverrideModes.Static)
        {
            return normalized;
        }

        return NetworkOverrideModes.Source;
    }

    private static string NormalizeOptionalValue(string value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
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

    private void SelectAllButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        foreach (ListViewItem item in _containerListView.Items)
        {
            item.Checked = true;
        }
    }

    private void SelectRelatedButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        var seed = GetCheckedContainers().ToList();
        if (seed.Count == 0)
        {
            var selected = GetSelectedContainer();
            if (selected != null)
            {
                seed.Add(selected);
            }
        }

        if (seed.Count == 0)
        {
            ShowError("Select or check at least one container first.");
            return;
        }

        var queue = new Queue<string>(seed.Select(item => item.Definition.Name));
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (!visited.Add(current))
            {
                continue;
            }

            SetCheckedState(current, true);
            var model = _containers.FirstOrDefault(item => string.Equals(item.Definition.Name, current, StringComparison.OrdinalIgnoreCase));
            if (model == null || model.Relations == null)
            {
                continue;
            }

            foreach (var relation in model.Relations)
            {
                queue.Enqueue(relation.Name);
            }
        }
    }

    private void ClearSelectionButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        foreach (ListViewItem item in _containerListView.Items)
        {
            item.Checked = false;
        }
    }

    private void ContainerSelectionChanged(object sender, EventArgs e)
    {
        PersistCurrentNetworkEditor();
        var model = GetSelectedContainer();
        LoadNetworkEditor(model);
        RefreshSelectedContainerDetails();
    }

    private void ContainerCheckedChanged(object sender, ItemCheckedEventArgs e)
    {
        if (_suppressItemCheckedEvents)
        {
            return;
        }

        RefreshRiskHighlighting();
        UpdateSelectionSummary();
    }

    private void PopulateContainerList()
    {
        _suppressItemCheckedEvents = true;
        _containerListView.BeginUpdate();
        _containerListView.Items.Clear();

        foreach (var model in _containers.OrderBy(item => item.Definition.Name, StringComparer.OrdinalIgnoreCase))
        {
            var item = new ListViewItem(model.Definition.Name);
            item.Tag = model;
            item.SubItems.Add(string.IsNullOrWhiteSpace(model.Definition.Status) ? "unknown" : model.Definition.Status);
            item.SubItems.Add(model.Definition.Image ?? string.Empty);
            item.SubItems.Add(FormatGigabytes(model.Definition.EstimatedMigrationBytes));
            item.SubItems.Add(model.GroupName ?? "standalone");
            item.SubItems.Add(string.Join(", ", model.Definition.Networks));
            item.SubItems.Add(GuiFormatter.BuildRelationSummary(model));
            _containerListView.Items.Add(item);
        }

        _containerListView.EndUpdate();
        _suppressItemCheckedEvents = false;
        if (_containerListView.Items.Count > 0)
        {
            _containerListView.Items[0].Selected = true;
        }
        else
        {
            LoadNetworkEditor(null);
            RefreshSelectedContainerDetails();
        }

        RefreshRiskHighlighting();
        UpdateSelectionSummary();
    }

    private void UpdateSelectionSummary()
    {
        var selectedCount = _containerListView.CheckedItems.Count;
        var riskCount = GetRiskySelectedContainerCount();
        _selectionLabel.Text = "Selected: " + selectedCount + " / " + _containers.Count + " | Risk: " + riskCount + " | Size: " + FormatGigabytes(CalculateSelectedMigrationBytes());

        if (_busy)
        {
            return;
        }

        _selectAllButton.Enabled = _containers.Count > 0;
        _selectRelatedButton.Enabled = _containers.Count > 0;
        _clearSelectionButton.Enabled = _containers.Count > 0;
        _startMigrationButton.Enabled = selectedCount > 0;
        UpdateRunButtonText();
    }

    private void RefreshRiskHighlighting()
    {
        var checkedNames = new HashSet<string>(GetCheckedContainers().Select(item => item.Definition.Name), StringComparer.OrdinalIgnoreCase);
        var missingNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var model in _containers)
        {
            model.HasSelectionRisk = checkedNames.Contains(model.Definition.Name) &&
                                     model.Relations.Any(item => !checkedNames.Contains(item.Name));
            model.IsMissingRelatedSelection = false;
            model.MissingRelatedNames = model.Relations
                .Where(item => !checkedNames.Contains(item.Name))
                .Select(item => item.Name)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (checkedNames.Contains(model.Definition.Name))
            {
                foreach (var missing in model.MissingRelatedNames)
                {
                    missingNames.Add(missing);
                }
            }
        }

        foreach (var model in _containers)
        {
            model.IsMissingRelatedSelection = !checkedNames.Contains(model.Definition.Name) && missingNames.Contains(model.Definition.Name);
        }

        _containerListView.BeginUpdate();
        foreach (ListViewItem item in _containerListView.Items)
        {
            var model = item.Tag as ContainerViewModel;
            if (model == null)
            {
                continue;
            }

            item.UseItemStyleForSubItems = true;
            item.Font = model.HasSelectionRisk ? _listFontBold : _listFontRegular;

            if (item.Checked && model.HasSelectionRisk)
            {
                item.BackColor = RiskSelectionColor;
                item.ForeColor = TextColor;
            }
            else if (item.Checked)
            {
                item.BackColor = SafeSelectionColor;
                item.ForeColor = TextColor;
            }
            else if (model.IsMissingRelatedSelection)
            {
                item.BackColor = RelatedSelectionColor;
                item.ForeColor = TextColor;
            }
            else
            {
                item.BackColor = SurfaceColor;
                item.ForeColor = TextColor;
            }
        }

        _containerListView.EndUpdate();
    }

    private int GetRiskySelectedContainerCount()
    {
        return _containers.Count(item => item.HasSelectionRisk);
    }

    private long CalculateSelectedMigrationBytes()
    {
        var selected = GetCheckedContainers().ToList();
        if (selected.Count == 0)
        {
            return 0;
        }

        var imageBytes = selected
            .Where(item => item != null && item.Definition != null && !string.IsNullOrWhiteSpace(item.Definition.Image))
            .GroupBy(item => item.Definition.Image, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First().Definition.EstimatedImageBytes)
            .Sum();

        var dataBytes = selected
            .Where(item => item != null && item.Definition != null)
            .Select(item => item.Definition.EstimatedDataBytes)
            .Sum();

        return imageBytes + dataBytes;
    }

    private void InitializeMigrationProgress(List<ContainerViewModel> selected)
    {
        _migrationContainerBytes.Clear();
        _migrationImageBytes.Clear();
        _migrationContainerFractions.Clear();
        _migrationImageFractions.Clear();
        _completedMigrationContainers.Clear();
        _completedMigrationImages.Clear();
        _migrationCompletedBytes = 0;
        _migrationStartedUtc = DateTime.UtcNow;
        _migrationProgressActive = true;
        _lastProgressMessage = _dryRunCheckBox.Checked ? "Building dry run plan..." : "Migrating selected containers...";

        foreach (var model in selected.Where(item => item != null && item.Definition != null))
        {
            _migrationContainerBytes[model.Definition.Name] = Math.Max(0, model.Definition.EstimatedDataBytes);
            _migrationContainerFractions[model.Definition.Name] = 0D;

            if (!string.IsNullOrWhiteSpace(model.Definition.Image) && !_migrationImageBytes.ContainsKey(model.Definition.Image))
            {
                _migrationImageBytes[model.Definition.Image] = Math.Max(0, model.Definition.EstimatedImageBytes);
                _migrationImageFractions[model.Definition.Image] = 0D;
            }
        }

        _migrationTotalBytes = _migrationContainerBytes.Values.Sum() + _migrationImageBytes.Values.Sum();
        RecalculateMigrationCompletedBytes();
    }

    private void ResetMigrationProgress()
    {
        _migrationProgressActive = false;
        _migrationTotalBytes = 0;
        _migrationCompletedBytes = 0;
        _lastProgressMessage = null;
        _migrationContainerBytes.Clear();
        _migrationImageBytes.Clear();
        _migrationContainerFractions.Clear();
        _migrationImageFractions.Clear();
        _completedMigrationContainers.Clear();
        _completedMigrationImages.Clear();
    }

    private void UpdateMigrationProgressFromCoreLog(string message)
    {
        if (!_migrationProgressActive)
        {
            return;
        }

        _lastProgressMessage = NormalizeOptionalValue(message) ?? _busyStatusText;

        const string restoredPrefix = "Container data restored for ";
        if (message != null && message.StartsWith(restoredPrefix, StringComparison.OrdinalIgnoreCase))
        {
            var containerName = NormalizeOptionalValue(message.Substring(restoredPrefix.Length));
            if (containerName != null && _completedMigrationContainers.Add(containerName))
            {
                UpdateContainerProgressFraction(containerName, 1D);
            }
        }

        const string imageReadyPrefix = "Image ready on Debian ";
        if (message != null && message.StartsWith(imageReadyPrefix, StringComparison.OrdinalIgnoreCase))
        {
            var imageName = NormalizeOptionalValue(message.Substring(imageReadyPrefix.Length));
            if (imageName != null && _completedMigrationImages.Add(imageName))
            {
                UpdateImageProgressFraction(imageName, 1D);
            }
        }

        string backupContainerName;
        if (TryParseContainerMountStage(message, "Backing up ", out backupContainerName))
        {
            UpdateContainerProgressFraction(backupContainerName, 0.35D);
        }

        string restoreContainerName;
        if (TryParseContainerMountStage(message, "Restoring ", out restoreContainerName))
        {
            UpdateContainerProgressFraction(restoreContainerName, 0.8D);
        }

        const string exportingImagePrefix = "Exporting image ";
        if (message != null && message.StartsWith(exportingImagePrefix, StringComparison.OrdinalIgnoreCase))
        {
            UpdateImageProgressFraction(NormalizeOptionalValue(message.Substring(exportingImagePrefix.Length)), 0.35D);
        }

        const string loadingImagePrefix = "Loading image ";
        if (message != null && message.StartsWith(loadingImagePrefix, StringComparison.OrdinalIgnoreCase))
        {
            UpdateImageProgressFraction(NormalizeOptionalValue(message.Substring(loadingImagePrefix.Length)), 0.8D);
        }

        string existingImageName;
        if (TryParseImageStage(message, "Image ", " already exists on Debian, skipping transfer.", out existingImageName))
        {
            UpdateImageProgressFraction(existingImageName, 0.95D);
        }

        string pulledImageName;
        if (TryParseImageStage(message, "Image ", " pulled successfully on Debian, skipping archive load.", out pulledImageName))
        {
            UpdateImageProgressFraction(pulledImageName, 0.95D);
        }

        RecalculateMigrationCompletedBytes();

        if (InvokeRequired)
        {
            BeginInvoke((MethodInvoker)delegate { UpdateProgressDisplay(); });
        }
        else
        {
            UpdateProgressDisplay();
        }
    }

    private void UpdateProgressDisplay()
    {
        if (!_migrationProgressActive)
        {
            return;
        }

        if (_migrationTotalBytes > 0)
        {
            _progressBar.Style = ProgressBarStyle.Continuous;
            _progressBar.Minimum = 0;
            _progressBar.Maximum = 1000;
            var ratio = Math.Min(1D, Math.Max(0D, (double)_migrationCompletedBytes / _migrationTotalBytes));
            _progressBar.Value = Math.Min(_progressBar.Maximum, (int)Math.Round(ratio * _progressBar.Maximum));
        }
        else
        {
            _progressBar.Style = ProgressBarStyle.Marquee;
        }

        var etaText = BuildEtaText();
        var progressText = _migrationTotalBytes > 0
            ? FormatProgressPercent((double)_migrationCompletedBytes * 100D / _migrationTotalBytes)
            : "progress: calculating";
        _statusLabel.Text = (_lastProgressMessage ?? _busyStatusText ?? "Migrating...") + " | " + progressText + " | " + etaText;
    }

    private string BuildEtaText()
    {
        if (!_migrationProgressActive)
        {
            return string.Empty;
        }

        if (_migrationTotalBytes <= 0 || _migrationCompletedBytes <= 0)
        {
            return "ETA: calculating...";
        }

        var elapsed = DateTime.UtcNow - _migrationStartedUtc;
        if (elapsed.TotalSeconds < 1)
        {
            return "ETA: calculating...";
        }

        var bytesPerSecond = _migrationCompletedBytes / elapsed.TotalSeconds;
        if (bytesPerSecond <= 1)
        {
            return "ETA: calculating...";
        }

        var remainingBytes = Math.Max(0, _migrationTotalBytes - _migrationCompletedBytes);
        if (remainingBytes == 0)
        {
            return "ETA: finishing...";
        }

        var remaining = TimeSpan.FromSeconds(remainingBytes / bytesPerSecond);
        return "ETA: " + FormatDuration(remaining);
    }

    private static string FormatDuration(TimeSpan duration)
    {
        if (duration.TotalHours >= 1)
        {
            return ((int)duration.TotalHours).ToString() + duration.ToString(@"\:mm\:ss");
        }

        return duration.ToString(@"m\:ss");
    }

    private void UpdateContainerProgressFraction(string containerName, double fraction)
    {
        var normalized = NormalizeOptionalValue(containerName);
        if (normalized == null || !_migrationContainerFractions.ContainsKey(normalized))
        {
            return;
        }

        _migrationContainerFractions[normalized] = Math.Max(_migrationContainerFractions[normalized], ClampProgressFraction(fraction));
    }

    private void UpdateImageProgressFraction(string imageName, double fraction)
    {
        var normalized = NormalizeOptionalValue(imageName);
        if (normalized == null || !_migrationImageFractions.ContainsKey(normalized))
        {
            return;
        }

        _migrationImageFractions[normalized] = Math.Max(_migrationImageFractions[normalized], ClampProgressFraction(fraction));
    }

    private void RecalculateMigrationCompletedBytes()
    {
        long completedBytes = 0;

        foreach (var item in _migrationContainerBytes)
        {
            double fraction;
            if (_migrationContainerFractions.TryGetValue(item.Key, out fraction))
            {
                completedBytes += (long)Math.Round(item.Value * ClampProgressFraction(fraction));
            }
        }

        foreach (var item in _migrationImageBytes)
        {
            double fraction;
            if (_migrationImageFractions.TryGetValue(item.Key, out fraction))
            {
                completedBytes += (long)Math.Round(item.Value * ClampProgressFraction(fraction));
            }
        }

        _migrationCompletedBytes = Math.Min(_migrationTotalBytes, Math.Max(0, completedBytes));
    }

    private static bool TryParseContainerMountStage(string message, string prefix, out string containerName)
    {
        containerName = null;
        if (string.IsNullOrWhiteSpace(message) || !message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var remainder = message.Substring(prefix.Length);
        var separatorIndex = remainder.IndexOf(':');
        if (separatorIndex <= 0)
        {
            return false;
        }

        containerName = NormalizeOptionalValue(remainder.Substring(0, separatorIndex));
        return containerName != null;
    }

    private static bool TryParseImageStage(string message, string prefix, string suffix, out string imageName)
    {
        imageName = null;
        if (string.IsNullOrWhiteSpace(message) ||
            !message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
            !message.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var length = message.Length - prefix.Length - suffix.Length;
        if (length <= 0)
        {
            return false;
        }

        imageName = NormalizeOptionalValue(message.Substring(prefix.Length, length));
        return imageName != null;
    }

    private static double ClampProgressFraction(double value)
    {
        return Math.Max(0D, Math.Min(1D, value));
    }

    private static string FormatProgressPercent(double percent)
    {
        var normalized = Math.Max(0D, Math.Min(100D, percent));
        return normalized >= 10D
            ? normalized.ToString("0", System.Globalization.CultureInfo.InvariantCulture) + "%"
            : normalized.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture) + "%";
    }

    private static string FormatGigabytes(long bytes)
    {
        if (bytes <= 0)
        {
            return "0.00";
        }

        return (bytes / 1073741824D).ToString("0.00");
    }

    private void UpdateRunButtonText()
    {
        _startMigrationButton.Text = _dryRunCheckBox.Checked ? "Build Dry Run Plan" : "Start Migration";
        _startMigrationButton.BackColor = _dryRunCheckBox.Checked ? Color.FromArgb(186, 120, 33) : AccentColor;
    }

    private void RunWorker(string busyText, Action action, Action completed)
    {
        SetBusy(true, busyText);

        ThreadPool.QueueUserWorkItem(
            delegate
            {
                var previousHandler = MigratorCore.LogHandler;
                MigratorCore.LogHandler = HandleCoreLog;

                try
                {
                    action();
                    BeginInvoke((MethodInvoker)delegate
                    {
                        completed();
                        SetBusy(false, "Ready.");
                    });
                }
                catch (Exception ex)
                {
                    HandleCoreLog("[!] " + ex.Message);
                    BeginInvoke((MethodInvoker)delegate
                    {
                        SetBusy(false, "Failed.");
                        ShowError(ex.Message);
                    });
                }
                finally
                {
                    MigratorCore.LogHandler = previousHandler;
                }
            });
    }

    private void SetBusy(bool busy, string statusText)
    {
        _busy = busy;
        _busyStatusText = statusText;
        _loadContainersButton.Enabled = !busy;
        _selectAllButton.Enabled = !busy && _containers.Count > 0;
        _selectRelatedButton.Enabled = !busy && _containers.Count > 0;
        _clearSelectionButton.Enabled = !busy && _containers.Count > 0;
        _startMigrationButton.Enabled = !busy && _containerListView.CheckedItems.Count > 0;
        _containerListView.Enabled = !busy;
        if (!busy)
        {
            UpdateNetworkEditorState(GetSelectedContainer());
            UpdateTargetNetworkButtonState();
        }
        else
        {
            _networkModeComboBox.Enabled = false;
            _networkNameComboBox.Enabled = false;
            _networkIpv4TextBox.Enabled = false;
            _networkIpv6TextBox.Enabled = false;
            _refreshNetworksButton.Enabled = false;
            _refreshNetworkInventoryButton.Enabled = false;
            _copyMacvlanButton.Enabled = false;
            _copyIpvlanButton.Enabled = false;
            _createMacvlanOnDebianButton.Enabled = false;
            _createIpvlanOnDebianButton.Enabled = false;
            _createAndUseMacvlanOnDebianButton.Enabled = false;
            _createAndUseIpvlanOnDebianButton.Enabled = false;
            _useCreatedNetworkButton.Enabled = false;
            _deleteNetworkButton.Enabled = false;
            _useSelectedNetworkButton.Enabled = false;
            _networkInventoryListView.Enabled = false;
            _loadTargetContainersButton.Enabled = false;
            _startTargetContainerButton.Enabled = false;
            _stopTargetContainerButton.Enabled = false;
            _deleteTargetContainerButton.Enabled = false;
            _refreshTargetContainerNetworksButton.Enabled = false;
            _applyTargetContainerNetworkButton.Enabled = false;
            _targetContainerNetworkModeComboBox.Enabled = false;
            _targetContainerNetworkNameComboBox.Enabled = false;
            _targetContainerNetworkIpv4TextBox.Enabled = false;
            _targetContainerNetworkIpv6TextBox.Enabled = false;
            _targetContainerListView.Enabled = false;
        }
        if (!busy)
        {
            ResetMigrationProgress();
            _progressBar.Style = ProgressBarStyle.Blocks;
            _progressBar.Minimum = 0;
            _progressBar.Maximum = 1000;
            _progressBar.Value = 0;
            _statusLabel.Text = statusText;
        }
        else if (_migrationProgressActive)
        {
            UpdateProgressDisplay();
        }
        else
        {
            _progressBar.Style = ProgressBarStyle.Marquee;
            _statusLabel.Text = statusText;
        }
        UpdateRunButtonText();
    }

    private void HandleCoreLog(string message)
    {
        UpdateMigrationProgressFromCoreLog(message);
        AppendLogLine(message);
    }

    private void AppendLogLine(string message)
    {
        var line = DateTime.Now.ToString("HH:mm:ss") + " " + message;
        _logger.WriteLine(line);

        if (InvokeRequired)
        {
            BeginInvoke((MethodInvoker)delegate { AppendLogToUi(line); });
            return;
        }

        AppendLogToUi(line);
    }

    private void AppendLogToUi(string line)
    {
        _logTextBox.AppendText(line + Environment.NewLine);
        _logTextBox.SelectionStart = _logTextBox.TextLength;
        _logTextBox.ScrollToCaret();
        if (_migrationProgressActive)
        {
            UpdateProgressDisplay();
        }
        else
        {
            _statusLabel.Text = line;
        }
    }

    private void UpdateTargetNetworkOptions(List<string> networks)
    {
        var selectedText = _networkNameComboBox.Text;
        var selectedTargetText = _targetContainerNetworkNameComboBox.Text;
        _targetDockerNetworks.Clear();
        if (networks != null)
        {
            _targetDockerNetworks.AddRange(networks);
        }

        _suppressNetworkEditorEvents = true;
        _suppressTargetContainerNetworkEditorEvents = true;
        _networkNameComboBox.BeginUpdate();
        _networkNameComboBox.Items.Clear();
        _targetContainerNetworkNameComboBox.BeginUpdate();
        _targetContainerNetworkNameComboBox.Items.Clear();
        foreach (var network in _targetDockerNetworks)
        {
            _networkNameComboBox.Items.Add(network);
            _targetContainerNetworkNameComboBox.Items.Add(network);
        }

        _networkNameComboBox.Text = selectedText;
        _targetContainerNetworkNameComboBox.Text = selectedTargetText;
        _networkNameComboBox.EndUpdate();
        _targetContainerNetworkNameComboBox.EndUpdate();
        _suppressNetworkEditorEvents = false;
        _suppressTargetContainerNetworkEditorEvents = false;
    }

    private bool TryBuildTargetConnectionInfo(out ConnectionInfoData target)
    {
        target = null;
        if (string.IsNullOrWhiteSpace(_targetHostTextBox.Text) ||
            string.IsNullOrWhiteSpace(_targetPortTextBox.Text) ||
            string.IsNullOrWhiteSpace(_targetUserTextBox.Text) ||
            string.IsNullOrWhiteSpace(_targetPasswordTextBox.Text))
        {
            return false;
        }

        try
        {
            target = BuildConnectionInfo(_targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox, "Debian");
            return true;
        }
        catch
        {
            target = null;
            return false;
        }
    }

    private ConnectionInfoData BuildConnectionInfo(TextBox hostTextBox, TextBox portTextBox, TextBox userTextBox, TextBox passwordTextBox, string sideName)
    {
        var host = (hostTextBox.Text ?? string.Empty).Trim();
        var user = (userTextBox.Text ?? string.Empty).Trim();
        var passwordText = passwordTextBox.Text ?? string.Empty;
        int port;

        if (host.Length == 0)
        {
            throw new InvalidOperationException(sideName + " host is required.");
        }

        if (!int.TryParse((portTextBox.Text ?? string.Empty).Trim(), out port) || port <= 0)
        {
            throw new InvalidOperationException(sideName + " SSH port is invalid.");
        }

        if (user.Length == 0)
        {
            throw new InvalidOperationException(sideName + " login is required.");
        }

        if (passwordText.Length == 0)
        {
            throw new InvalidOperationException(sideName + " password is required.");
        }

        return new ConnectionInfoData
        {
            Host = host,
            Port = port,
            Username = user,
            Password = ToSecureString(passwordText)
        };
    }

    private static SecureString ToSecureString(string value)
    {
        var result = new SecureString();
        foreach (var ch in value ?? string.Empty)
        {
            result.AppendChar(ch);
        }

        result.MakeReadOnly();
        return result;
    }

    private string NormalizeTargetRoot()
    {
        var text = (_targetRootTextBox.Text ?? string.Empty).Trim();
        if (text.Length == 0)
        {
            text = MigratorCore.DefaultTargetRoot;
            _targetRootTextBox.Text = text;
        }

        return text;
    }

    private ContainerViewModel GetSelectedContainer()
    {
        return _containerListView.SelectedItems.Count == 0
            ? null
            : _containerListView.SelectedItems[0].Tag as ContainerViewModel;
    }

    private IEnumerable<ContainerViewModel> GetCheckedContainers()
    {
        foreach (ListViewItem item in _containerListView.CheckedItems)
        {
            var model = item.Tag as ContainerViewModel;
            if (model != null)
            {
                yield return model;
            }
        }
    }

    private List<string> GetMissingRelatedContainers(List<ContainerViewModel> selected)
    {
        var selectedNames = new HashSet<string>(selected.Select(item => item.Definition.Name), StringComparer.OrdinalIgnoreCase);
        return selected
            .SelectMany(item => item.Relations ?? new List<ContainerRelation>())
            .Select(item => item.Name)
            .Where(name => !selectedNames.Contains(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private void SetCheckedState(string containerName, bool isChecked)
    {
        foreach (ListViewItem item in _containerListView.Items)
        {
            if (string.Equals(item.Text, containerName, StringComparison.OrdinalIgnoreCase))
            {
                item.Checked = isChecked;
                return;
            }
        }
    }

    private void ShowError(string message)
    {
        MessageBox.Show(this, message, "Docker Synology Migrator", MessageBoxButtons.OK, MessageBoxIcon.Error);
    }
}

internal sealed class SessionLogger
{
    private readonly object _sync = new object();

    internal SessionLogger(string path)
    {
        Path = path;
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(path));
    }

    internal string Path { get; private set; }

    internal void WriteLine(string line)
    {
        lock (_sync)
        {
            File.AppendAllText(Path, line + Environment.NewLine, Encoding.UTF8);
        }
    }
}

internal sealed class ConnectionProfile
{
    public string SourceHost { get; set; }
    public string SourcePort { get; set; }
    public string SourceLogin { get; set; }
    public string SourcePassword { get; set; }
    public string TargetHost { get; set; }
    public string TargetPort { get; set; }
    public string TargetLogin { get; set; }
    public string TargetPassword { get; set; }
    public string TargetRoot { get; set; }
    public bool SavePasswords { get; set; }
    public string CreatorNetworkName { get; set; }
    public string CreatorParentInterface { get; set; }
    public string CreatorSubnet { get; set; }
    public string CreatorGateway { get; set; }
    public string CreatorIpRange { get; set; }
    public string CreatorIpvlanMode { get; set; }
    public string VmSourceHost { get; set; }
    public string VmSourcePort { get; set; }
    public string VmSourceLogin { get; set; }
    public string VmSourcePassword { get; set; }
    public string VmTargetHost { get; set; }
    public string VmTargetPort { get; set; }
    public string VmTargetLogin { get; set; }
    public string VmTargetPassword { get; set; }
    public string VmTargetRoot { get; set; }
    public string VmTargetStorage { get; set; }
    public string VmTargetBridge { get; set; }
    public bool VmStopSourceDuringExport { get; set; }
    public bool VmStartImportedVirtualMachines { get; set; }
    public bool VmSavePasswords { get; set; }
    public int? VmWindowLeft { get; set; }
    public int? VmWindowTop { get; set; }
    public int? VmWindowWidth { get; set; }
    public int? VmWindowHeight { get; set; }
    public string VmWindowState { get; set; }
    public int? VmWorkspaceSplitterDistance { get; set; }
    public int? VmMainSplitterDistance { get; set; }
    public int? VmTargetSplitterDistance { get; set; }
    public int[] VmSourceVirtualMachineColumnWidths { get; set; }
    public int[] VmTargetVirtualMachineColumnWidths { get; set; }
    public int? WindowLeft { get; set; }
    public int? WindowTop { get; set; }
    public int? WindowWidth { get; set; }
    public int? WindowHeight { get; set; }
    public string WindowState { get; set; }
    public int? WorkspaceSplitterDistance { get; set; }
    public int? MainSplitterDistance { get; set; }
    public int? TargetContainerSplitterDistance { get; set; }
    public int? NetworkInventorySplitterDistance { get; set; }
    public int[] SourceContainerColumnWidths { get; set; }
    public int[] TargetContainerColumnWidths { get; set; }
    public int[] NetworkInventoryColumnWidths { get; set; }
}

internal sealed class ContainerViewModel
{
    internal ContainerDefinition Definition { get; set; }
    internal string GroupName { get; set; }
    internal List<ContainerRelation> Relations { get; set; }
    internal ContainerNetworkOverride NetworkOverride { get; set; }
    internal bool HasSelectionRisk { get; set; }
    internal bool IsMissingRelatedSelection { get; set; }
    internal List<string> MissingRelatedNames { get; set; }
}

internal sealed class VirtualMachineViewModel
{
    internal VirtualMachineDefinition Definition { get; set; }
}

internal sealed class ContainerRelation
{
    internal string Name { get; set; }
    internal string Reason { get; set; }
}

internal sealed class IpReferenceField
{
    internal string Name { get; set; }
    internal string Value { get; set; }
}

internal sealed class DhcpIpReferenceWarning
{
    internal string ReferencingContainerName { get; set; }
    internal string ReferencedContainerName { get; set; }
    internal string Address { get; set; }
    internal string FieldName { get; set; }
    internal string Sample { get; set; }
}

internal static class ContainerRelationshipBuilder
{
    private static readonly string[] DefaultNetworks = { "bridge", "host", "none" };

    internal static List<ContainerViewModel> Build(List<ContainerDefinition> definitions)
    {
        var map = definitions.ToDictionary(item => item.Name, StringComparer.OrdinalIgnoreCase);
        var reasons = definitions.ToDictionary(item => item.Name, item => new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

        BuildComposeProjectRelations(definitions, reasons);
        BuildNetworkRelations(definitions, reasons);
        BuildComposeDependsRelations(definitions, map, reasons);

        return definitions
            .Select(definition => new ContainerViewModel
            {
                Definition = definition,
                GroupName = BuildGroupName(definition),
                Relations = BuildRelationsForContainer(definition, reasons),
                NetworkOverride = BuildDefaultNetworkOverride(definition)
            })
            .OrderBy(item => item.Definition.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static void BuildComposeProjectRelations(List<ContainerDefinition> definitions, Dictionary<string, Dictionary<string, List<string>>> reasons)
    {
        var groups = definitions
            .Where(item => !string.IsNullOrWhiteSpace(item.ComposeProject))
            .GroupBy(item => item.ComposeProject, StringComparer.OrdinalIgnoreCase);

        foreach (var group in groups)
        {
            var containers = group.ToList();
            for (var i = 0; i < containers.Count; i++)
            {
                for (var j = i + 1; j < containers.Count; j++)
                {
                    AddReason(reasons, containers[i].Name, containers[j].Name, "compose project: " + group.Key);
                    AddReason(reasons, containers[j].Name, containers[i].Name, "compose project: " + group.Key);
                }
            }
        }
    }

    private static void BuildNetworkRelations(List<ContainerDefinition> definitions, Dictionary<string, Dictionary<string, List<string>>> reasons)
    {
        var groups = definitions
            .SelectMany(item => item.Networks
                .Where(network => !DefaultNetworks.Contains(network, StringComparer.OrdinalIgnoreCase))
                .Select(network => new { Container = item, Network = network }))
            .GroupBy(item => item.Network, StringComparer.OrdinalIgnoreCase);

        foreach (var group in groups)
        {
            var containers = group.Select(item => item.Container).Distinct().ToList();
            for (var i = 0; i < containers.Count; i++)
            {
                for (var j = i + 1; j < containers.Count; j++)
                {
                    AddReason(reasons, containers[i].Name, containers[j].Name, "shared network: " + group.Key);
                    AddReason(reasons, containers[j].Name, containers[i].Name, "shared network: " + group.Key);
                }
            }
        }
    }

    private static void BuildComposeDependsRelations(List<ContainerDefinition> definitions, Dictionary<string, ContainerDefinition> map, Dictionary<string, Dictionary<string, List<string>>> reasons)
    {
        foreach (var definition in definitions)
        {
            string raw;
            if (!definition.Labels.TryGetValue("com.docker.compose.depends_on", out raw) || string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            foreach (var dependencyName in ParseDependencyNames(raw))
            {
                var target = map.Values.FirstOrDefault(item =>
                    string.Equals(item.Name, dependencyName, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(item.Name, definition.ComposeProject + "-" + dependencyName + "-1", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(item.Name, definition.ComposeProject + "_" + dependencyName + "_1", StringComparison.OrdinalIgnoreCase));

                if (target == null)
                {
                    continue;
                }

                AddReason(reasons, definition.Name, target.Name, "compose depends_on");
                AddReason(reasons, target.Name, definition.Name, "compose depends_on");
            }
        }
    }

    private static IEnumerable<string> ParseDependencyNames(string raw)
    {
        return raw
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(item =>
            {
                var trimmed = item.Trim();
                var index = trimmed.IndexOf(':');
                return index > 0 ? trimmed.Substring(0, index).Trim() : trimmed;
            })
            .Where(item => item.Length > 0);
    }

    private static string BuildGroupName(ContainerDefinition definition)
    {
        if (!string.IsNullOrWhiteSpace(definition.ComposeProject))
        {
            return definition.ComposeProject;
        }

        var customNetwork = definition.Networks.FirstOrDefault(network => !DefaultNetworks.Contains(network, StringComparer.OrdinalIgnoreCase));
        return string.IsNullOrWhiteSpace(customNetwork) ? "standalone" : customNetwork;
    }

    private static ContainerNetworkOverride BuildDefaultNetworkOverride(ContainerDefinition definition)
    {
        var preferredAttachment = definition.NetworkAttachments
            .FirstOrDefault(item => item != null && !string.IsNullOrWhiteSpace(item.Name) && !DefaultNetworks.Contains(item.Name, StringComparer.OrdinalIgnoreCase)) ??
            definition.NetworkAttachments.FirstOrDefault(item => item != null && !string.IsNullOrWhiteSpace(item.Name));

        var preferredNetworkName = preferredAttachment == null ? definition.Networks.FirstOrDefault() : preferredAttachment.Name;
        if (string.IsNullOrWhiteSpace(preferredNetworkName) || DefaultNetworks.Contains(preferredNetworkName, StringComparer.OrdinalIgnoreCase))
        {
            preferredNetworkName = null;
        }

        return new ContainerNetworkOverride
        {
            ContainerName = definition.Name,
            Mode = NetworkOverrideModes.Source,
            NetworkName = preferredNetworkName,
            IPv4Address = preferredAttachment == null ? null : preferredAttachment.IPv4Address,
            IPv6Address = preferredAttachment == null ? null : preferredAttachment.IPv6Address
        };
    }

    private static List<ContainerRelation> BuildRelationsForContainer(ContainerDefinition definition, Dictionary<string, Dictionary<string, List<string>>> reasons)
    {
        Dictionary<string, List<string>> itemReasons;
        if (!reasons.TryGetValue(definition.Name, out itemReasons))
        {
            return new List<ContainerRelation>();
        }

        return itemReasons
            .OrderBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
            .Select(item => new ContainerRelation
            {
                Name = item.Key,
                Reason = string.Join("; ", item.Value.Distinct(StringComparer.OrdinalIgnoreCase))
            })
            .ToList();
    }

    private static void AddReason(Dictionary<string, Dictionary<string, List<string>>> reasons, string source, string target, string reason)
    {
        Dictionary<string, List<string>> targetReasons;
        if (!reasons.TryGetValue(source, out targetReasons))
        {
            targetReasons = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            reasons[source] = targetReasons;
        }

        List<string> reasonList;
        if (!targetReasons.TryGetValue(target, out reasonList))
        {
            reasonList = new List<string>();
            targetReasons[target] = reasonList;
        }

        reasonList.Add(reason);
    }
}

internal static class GuiFormatter
{
    internal static string BuildRelationSummary(ContainerViewModel model)
    {
        if (model.Relations == null || model.Relations.Count == 0)
        {
            return "none";
        }

        var names = model.Relations.Select(item => item.Name).Take(3).ToList();
        var suffix = model.Relations.Count > names.Count ? " +" + (model.Relations.Count - names.Count).ToString() : string.Empty;
        return string.Join(", ", names) + suffix;
    }

    internal static string BuildDetails(ContainerViewModel model)
    {
        var definition = model.Definition;
        var builder = new StringBuilder();

        AppendLine(builder, "Name", definition.Name);
        AppendLine(builder, "State", string.IsNullOrWhiteSpace(definition.Status) ? "unknown" : definition.Status);
        AppendLine(builder, "Image", definition.Image);
        AppendLine(builder, "Compose project", string.IsNullOrWhiteSpace(definition.ComposeProject) ? "-" : definition.ComposeProject);
        AppendLine(builder, "Group", string.IsNullOrWhiteSpace(model.GroupName) ? "standalone" : model.GroupName);
        AppendLine(builder, "Network mode", definition.NetworkMode);
        AppendLine(builder, "Hostname", definition.Hostname);
        AppendLine(builder, "User", definition.User);
        AppendLine(builder, "Working dir", definition.WorkingDir);
        AppendLine(builder, "Restart", string.IsNullOrWhiteSpace(definition.RestartPolicyName) ? "no" : definition.RestartPolicyName);
        AppendLine(builder, "TTY", definition.Tty ? "yes" : "no");
        AppendLine(builder, "Interactive stdin", definition.OpenStdin ? "yes" : "no");
        AppendLine(builder, "Privileged", definition.Privileged ? "yes" : "no");
        AppendLine(builder, "Migration network", BuildNetworkOverrideSummary(model));

        AppendSection(builder, "Networks", definition.Networks);
        AppendSection(builder, "Network attachments", BuildNetworkAttachmentLines(definition));
        AppendSection(builder, "Ports", definition.PortBindings.Select(item => string.IsNullOrWhiteSpace(item.HostIp) || item.HostIp == "0.0.0.0"
            ? item.HostPort + ":" + item.ContainerPort
            : item.HostIp + ":" + item.HostPort + ":" + item.ContainerPort));
        AppendSection(builder, "Command", definition.Command);
        AppendSection(builder, "Entrypoint", definition.Entrypoint);
        AppendSection(builder, "Environment", definition.Environment);
        AppendSection(builder, "Extra hosts", definition.ExtraHosts);
        AppendSection(builder, "Capabilities", definition.CapAdd);
        AppendSection(builder, "Devices", definition.Devices.Select(item => item.PathOnHost + " -> " + item.PathInContainer + (string.IsNullOrWhiteSpace(item.CgroupPermissions) ? string.Empty : " (" + item.CgroupPermissions + ")")));
        AppendSection(builder, "Related containers", model.Relations.Select(item => item.Name + " [" + item.Reason + "]"));
        AppendSection(builder, "Mounts", definition.Mounts.Select(BuildMountLine));
        AppendMapSection(builder, "Labels", definition.Labels);

        return builder.ToString().Trim();
    }

    internal static string BuildTargetDetails(ContainerDefinition definition)
    {
        var builder = new StringBuilder();

        AppendLine(builder, "Name", definition.Name);
        AppendLine(builder, "State", string.IsNullOrWhiteSpace(definition.Status) ? "unknown" : definition.Status);
        AppendLine(builder, "Image", definition.Image);
        AppendLine(builder, "Compose project", string.IsNullOrWhiteSpace(definition.ComposeProject) ? "-" : definition.ComposeProject);
        AppendLine(builder, "Network mode", definition.NetworkMode);
        AppendLine(builder, "Hostname", definition.Hostname);
        AppendLine(builder, "User", definition.User);
        AppendLine(builder, "Working dir", definition.WorkingDir);
        AppendLine(builder, "Restart", string.IsNullOrWhiteSpace(definition.RestartPolicyName) ? "no" : definition.RestartPolicyName);
        AppendLine(builder, "TTY", definition.Tty ? "yes" : "no");
        AppendLine(builder, "Interactive stdin", definition.OpenStdin ? "yes" : "no");
        AppendLine(builder, "Privileged", definition.Privileged ? "yes" : "no");

        AppendSection(builder, "Networks", definition.Networks);
        AppendSection(builder, "Network attachments", BuildNetworkAttachmentLines(definition));
        AppendSection(builder, "Ports", definition.PortBindings.Select(item => string.IsNullOrWhiteSpace(item.HostIp) || item.HostIp == "0.0.0.0"
            ? item.HostPort + ":" + item.ContainerPort
            : item.HostIp + ":" + item.HostPort + ":" + item.ContainerPort));
        AppendSection(builder, "Command", definition.Command);
        AppendSection(builder, "Entrypoint", definition.Entrypoint);
        AppendSection(builder, "Environment", definition.Environment);
        AppendSection(builder, "Extra hosts", definition.ExtraHosts);
        AppendSection(builder, "Capabilities", definition.CapAdd);
        AppendSection(builder, "Devices", definition.Devices.Select(item => item.PathOnHost + " -> " + item.PathInContainer + (string.IsNullOrWhiteSpace(item.CgroupPermissions) ? string.Empty : " (" + item.CgroupPermissions + ")")));
        AppendSection(builder, "Mounts", definition.Mounts.Select(BuildTargetMountLine));
        AppendMapSection(builder, "Labels", definition.Labels);

        return builder.ToString().Trim();
    }

    internal static string BuildNetworkNamesSummary(ContainerDefinition definition)
    {
        var names = (definition == null ? null : definition.Networks) ?? new List<string>();
        if (names.Count == 0)
        {
            return "none";
        }

        var ordered = names
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return ordered.Count == 0 ? "none" : string.Join(", ", ordered);
    }

    internal static string BuildIpAddressSummary(ContainerDefinition definition)
    {
        var addresses = ((definition == null ? null : definition.NetworkAttachments) ?? new List<ContainerNetworkAttachment>())
            .SelectMany(item => new[] { item == null ? null : item.IPv4Address, item == null ? null : item.IPv6Address })
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return addresses.Count == 0 ? "-" : string.Join(", ", addresses);
    }

    internal static string BuildNetworkIpamSummary(DockerNetworkDefinition definition)
    {
        var items = (definition == null ? null : definition.IpamConfigs) ?? new List<DockerNetworkIpamConfig>();
        var subnets = items
            .Select(item => item == null ? null : item.Subnet)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (subnets.Count == 0)
        {
            return "-";
        }

        if (subnets.Count == 1)
        {
            return subnets[0];
        }

        return subnets[0] + " +" + (subnets.Count - 1).ToString();
    }

    internal static string BuildDockerNetworkDetails(DockerNetworkDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        AppendLine(builder, "Name", definition.Name);
        AppendLine(builder, "Driver", definition.Driver);
        AppendLine(builder, "Scope", definition.Scope);
        AppendLine(builder, "Built-in", IsBuiltInDockerNetwork(definition.Name) ? "yes" : "no");
        AppendLine(builder, "Internal", definition.Internal ? "yes" : "no");
        AppendLine(builder, "Attachable", definition.Attachable ? "yes" : "no");
        AppendLine(builder, "IPv6", definition.EnableIPv6 ? "enabled" : "disabled");
        AppendLine(builder, "Parent", definition.ParentInterface);
        AppendLine(builder, "macvlan mode", definition.MacvlanMode);
        AppendLine(builder, "ipvlan mode", definition.IpvlanMode);

        AppendSection(builder, "IPAM", definition.IpamConfigs.Select(BuildNetworkIpamLine));
        AppendSection(builder, "Connected containers", definition.ConnectedContainers.Select(BuildConnectedContainerLine));
        AppendMapSection(builder, "Options", definition.Options);

        return builder.ToString().Trim();
    }

    private static string BuildMountLine(MountDefinition mount)
    {
        if (string.Equals(mount.Type, "bind", StringComparison.OrdinalIgnoreCase))
        {
            return mount.SourcePath + " -> " + mount.DestinationPath + " | target: " + mount.TargetSource + (mount.ReadOnly ? " | ro" : string.Empty);
        }

        return mount.VolumeName + " -> " + mount.DestinationPath + (mount.ReadOnly ? " | ro" : string.Empty);
    }

    private static string BuildTargetMountLine(MountDefinition mount)
    {
        if (string.Equals(mount.Type, "bind", StringComparison.OrdinalIgnoreCase))
        {
            return mount.SourcePath + " -> " + mount.DestinationPath + (mount.ReadOnly ? " | ro" : string.Empty);
        }

        return mount.VolumeName + " -> " + mount.DestinationPath + (mount.ReadOnly ? " | ro" : string.Empty);
    }

    private static string BuildNetworkOverrideSummary(ContainerViewModel model)
    {
        var networkOverride = model.NetworkOverride;
        if (networkOverride == null || string.Equals((networkOverride.Mode ?? string.Empty).Trim(), NetworkOverrideModes.Source, StringComparison.OrdinalIgnoreCase))
        {
            return "source";
        }

        if (string.Equals(networkOverride.Mode, NetworkOverrideModes.Host, StringComparison.OrdinalIgnoreCase))
        {
            return "host";
        }

        if (string.Equals(networkOverride.Mode, NetworkOverrideModes.Dhcp, StringComparison.OrdinalIgnoreCase))
        {
            return "dhcp -> " + (networkOverride.NetworkName ?? "-");
        }

        return "static -> " + (networkOverride.NetworkName ?? "-") +
               " | ipv4=" + (networkOverride.IPv4Address ?? "-") +
               " | ipv6=" + (networkOverride.IPv6Address ?? "-");
    }

    private static string BuildNetworkIpamLine(DockerNetworkIpamConfig config)
    {
        if (config == null)
        {
            return null;
        }

        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(config.Subnet))
        {
            parts.Add("subnet=" + config.Subnet);
        }

        if (!string.IsNullOrWhiteSpace(config.Gateway))
        {
            parts.Add("gateway=" + config.Gateway);
        }

        if (!string.IsNullOrWhiteSpace(config.IpRange))
        {
            parts.Add("ip-range=" + config.IpRange);
        }

        return parts.Count == 0 ? null : string.Join(" | ", parts);
    }

    private static string BuildConnectedContainerLine(DockerNetworkContainerReference item)
    {
        if (item == null)
        {
            return null;
        }

        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(item.Name))
        {
            parts.Add(item.Name);
        }

        if (!string.IsNullOrWhiteSpace(item.IPv4Address))
        {
            parts.Add("ipv4=" + item.IPv4Address);
        }

        if (!string.IsNullOrWhiteSpace(item.IPv6Address))
        {
            parts.Add("ipv6=" + item.IPv6Address);
        }

        return parts.Count == 0 ? null : string.Join(" | ", parts);
    }

    private static IEnumerable<string> BuildNetworkAttachmentLines(ContainerDefinition definition)
    {
        foreach (var attachment in definition.NetworkAttachments)
        {
            if (attachment == null || string.IsNullOrWhiteSpace(attachment.Name))
            {
                continue;
            }

            var parts = new List<string> { attachment.Name };
            if (!string.IsNullOrWhiteSpace(attachment.IPv4Address))
            {
                parts.Add("ipv4=" + attachment.IPv4Address);
            }

            if (!string.IsNullOrWhiteSpace(attachment.IPv6Address))
            {
                parts.Add("ipv6=" + attachment.IPv6Address);
            }

            if (attachment.Aliases != null && attachment.Aliases.Count > 0)
            {
                parts.Add("aliases=" + string.Join(", ", attachment.Aliases));
            }

            yield return string.Join(" | ", parts);
        }
    }

    private static bool IsBuiltInDockerNetwork(string value)
    {
        return string.Equals(value, "host", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(value, "bridge", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(value, "none", StringComparison.OrdinalIgnoreCase);
    }

    private static void AppendLine(StringBuilder builder, string label, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        builder.AppendLine(label + ": " + value);
    }

    private static void AppendSection(StringBuilder builder, string title, IEnumerable<string> lines)
    {
        var items = (lines ?? Enumerable.Empty<string>()).Where(item => !string.IsNullOrWhiteSpace(item)).ToList();
        if (items.Count == 0)
        {
            return;
        }

        builder.AppendLine();
        builder.AppendLine(title + ":");
        foreach (var item in items)
        {
            builder.AppendLine(" - " + item);
        }
    }

    private static void AppendMapSection(StringBuilder builder, string title, Dictionary<string, string> items)
    {
        if (items == null || items.Count == 0)
        {
            return;
        }

        builder.AppendLine();
        builder.AppendLine(title + ":");
        foreach (var item in items.OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase))
        {
            builder.AppendLine(" - " + item.Key + "=" + item.Value);
        }
    }
}
