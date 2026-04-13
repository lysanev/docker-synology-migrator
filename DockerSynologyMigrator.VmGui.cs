using System;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using System.Windows.Forms;

internal sealed class VmMigrationForm : Form
{
    private readonly MainForm _containersForm;
    private readonly TextBox _sourceHostTextBox = new TextBox();
    private readonly TextBox _sourcePortTextBox = new TextBox();
    private readonly TextBox _sourceUserTextBox = new TextBox();
    private readonly TextBox _sourcePasswordTextBox = new TextBox();
    private readonly TextBox _targetHostTextBox = new TextBox();
    private readonly TextBox _targetPortTextBox = new TextBox();
    private readonly TextBox _targetUserTextBox = new TextBox();
    private readonly TextBox _targetPasswordTextBox = new TextBox();
    private readonly TextBox _targetRootTextBox = new TextBox();
    private readonly ComboBox _targetStorageComboBox = new ComboBox();
    private readonly ComboBox _targetBridgeComboBox = new ComboBox();
    private readonly CheckBox _stopSourceVirtualMachinesCheckBox = new CheckBox();
    private readonly CheckBox _startImportedVirtualMachinesCheckBox = new CheckBox();
    private readonly CheckBox _dryRunCheckBox = new CheckBox();
    private readonly CheckBox _savePasswordsCheckBox = new CheckBox();
    private readonly Button _containersScreenButton = new Button();
    private readonly Button _loadVirtualMachinesButton = new Button();
    private readonly Button _selectAllButton = new Button();
    private readonly Button _clearSelectionButton = new Button();
    private readonly Button _loadProxmoxButton = new Button();
    private readonly Button _startMigrationButton = new Button();
    private readonly Button _startTargetVirtualMachineButton = new Button();
    private readonly Button _stopTargetVirtualMachineButton = new Button();
    private readonly Button _deleteTargetVirtualMachineButton = new Button();
    private readonly ListView _sourceVirtualMachineListView = new ListView();
    private readonly ListView _targetVirtualMachineListView = new ListView();
    private readonly TextBox _sourceDetailsTextBox = new TextBox();
    private readonly TextBox _targetDetailsTextBox = new TextBox();
    private readonly RichTextBox _logTextBox = new RichTextBox();
    private readonly Label _selectionLabel = new Label();
    private readonly Label _statusLabel = new Label();
    private readonly Label _logPathLabel = new Label();
    private readonly SplitContainer _workspaceSplitContainer = new SplitContainer();
    private readonly SplitContainer _mainSplitContainer = new SplitContainer();
    private readonly SplitContainer _targetVirtualMachineSplitContainer = new SplitContainer();
    private readonly string _profilePath;
    private readonly List<VirtualMachineViewModel> _virtualMachines = new List<VirtualMachineViewModel>();
    private readonly List<ProxmoxVirtualMachineDefinition> _targetVirtualMachines = new List<ProxmoxVirtualMachineDefinition>();
    private readonly List<string> _targetStorages = new List<string>();
    private readonly List<string> _targetBridges = new List<string>();
    private SessionLogger _logger;
    private bool _busy;
    private bool _suppressSourceItemCheckedEvents;
    private ConnectionProfile _pendingLayoutProfile;
    private bool _savedLayoutApplied;

    private static readonly Color AppBackColor = Color.FromArgb(245, 242, 235);
    private static readonly Color SurfaceColor = Color.FromArgb(255, 252, 247);
    private static readonly Color AccentColor = Color.FromArgb(33, 110, 122);
    private static readonly Color AccentDarkColor = Color.FromArgb(22, 78, 87);
    private static readonly Color SecondaryButtonColor = Color.FromArgb(233, 226, 213);
    private static readonly Color TextColor = Color.FromArgb(36, 42, 44);
    private static readonly JavaScriptSerializer Json = new JavaScriptSerializer();

    internal VmMigrationForm(MainForm containersForm)
    {
        _containersForm = containersForm;
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
        FormClosing += VmMigrationFormClosing;
        FormClosed += VmMigrationFormClosed;
        Shown += VmMigrationFormShown;

        BuildLayout();
        ApplyTheme();
        InitializeDefaults();
        InitializeLogger();
        LoadSavedProfile();
        UpdateSelectionSummary();
        UpdateUiState();
    }

    private static string BuildAppTitle()
    {
        return "Docker Synology Migrator v" + Program.AppVersion + " | Virtual Machines";
    }

    private void BuildLayout()
    {
        var root = new TableLayoutPanel();
        root.Dock = DockStyle.Fill;
        root.Padding = new Padding(14, 12, 14, 14);
        root.ColumnCount = 1;
        root.RowCount = 4;
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 76F));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 304F));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 68F));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));
        Controls.Add(root);

        root.Controls.Add(BuildHeaderPanel(), 0, 0);
        root.Controls.Add(BuildConnectionPanel(), 0, 1);
        root.Controls.Add(BuildActionPanel(), 0, 2);
        root.Controls.Add(BuildWorkspacePanel(), 0, 3);
    }

    private Control BuildHeaderPanel()
    {
        var panel = new Panel();
        panel.Dock = DockStyle.Fill;
        panel.Margin = new Padding(0, 0, 0, 10);
        panel.Padding = new Padding(20, 14, 20, 10);
        panel.BackColor = AccentDarkColor;

        var layout = new TableLayoutPanel();
        layout.Dock = DockStyle.Fill;
        layout.ColumnCount = 2;
        layout.RowCount = 1;
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160F));

        var title = new Label();
        title.Dock = DockStyle.Fill;
        title.ForeColor = Color.White;
        title.Font = new Font("Segoe UI Semibold", 18F, FontStyle.Bold, GraphicsUnit.Point);
        title.Text = BuildAppTitle();
        title.TextAlign = ContentAlignment.MiddleLeft;

        _containersScreenButton.Text = "Containers";
        _containersScreenButton.Dock = DockStyle.Right;
        _containersScreenButton.Margin = new Padding(0, 8, 0, 8);
        _containersScreenButton.Click += ContainersScreenButtonClick;

        layout.Controls.Add(title, 0, 0);
        layout.Controls.Add(_containersScreenButton, 1, 0);
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
        panel.Controls.Add(BuildConnectionGroup("Proxmox Target", _targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox), 1, 0);
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
        table.RowCount = 5;
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        for (var i = 0; i < 4; i++)
        {
            table.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        }
        table.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

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
        group.Text = "VM Migration Options";
        group.Dock = DockStyle.Fill;

        var table = new TableLayoutPanel();
        table.Dock = DockStyle.Fill;
        table.ColumnCount = 1;
        table.RowCount = 8;
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 30F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 30F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 30F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 28F));
        table.RowStyles.Add(new RowStyle(SizeType.Absolute, 68F));

        _targetStorageComboBox.DropDownStyle = ComboBoxStyle.DropDown;
        _targetBridgeComboBox.DropDownStyle = ComboBoxStyle.DropDown;
        _dryRunCheckBox.CheckedChanged += DryRunCheckBoxChanged;

        var targetRootPanel = new TableLayoutPanel();
        targetRootPanel.Dock = DockStyle.Fill;
        targetRootPanel.ColumnCount = 2;
        targetRootPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        targetRootPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        targetRootPanel.Controls.Add(BuildLabel("Target path"), 0, 0);
        targetRootPanel.Controls.Add(_targetRootTextBox, 1, 0);

        var storagePanel = new TableLayoutPanel();
        storagePanel.Dock = DockStyle.Fill;
        storagePanel.ColumnCount = 2;
        storagePanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        storagePanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        storagePanel.Controls.Add(BuildLabel("Storage"), 0, 0);
        storagePanel.Controls.Add(_targetStorageComboBox, 1, 0);

        var bridgePanel = new TableLayoutPanel();
        bridgePanel.Dock = DockStyle.Fill;
        bridgePanel.ColumnCount = 2;
        bridgePanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 96F));
        bridgePanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        bridgePanel.Controls.Add(BuildLabel("Bridge"), 0, 0);
        bridgePanel.Controls.Add(_targetBridgeComboBox, 1, 0);

        _stopSourceVirtualMachinesCheckBox.Text = "Temporarily stop source VMs during export";
        _stopSourceVirtualMachinesCheckBox.AutoSize = true;

        _startImportedVirtualMachinesCheckBox.Text = "Start imported VMs on Proxmox after migration";
        _startImportedVirtualMachinesCheckBox.AutoSize = true;

        _dryRunCheckBox.Text = "Dry Run: validate and build plan only";
        _dryRunCheckBox.AutoSize = true;

        _savePasswordsCheckBox.Text = "Save passwords in profile";
        _savePasswordsCheckBox.AutoSize = true;

        _logPathLabel.Dock = DockStyle.Fill;
        _logPathLabel.AutoEllipsis = true;
        _logPathLabel.TextAlign = ContentAlignment.MiddleLeft;
        _logPathLabel.Font = new Font("Segoe UI", 8.5F, FontStyle.Regular, GraphicsUnit.Point);

        table.Controls.Add(targetRootPanel, 0, 0);
        table.Controls.Add(storagePanel, 0, 1);
        table.Controls.Add(bridgePanel, 0, 2);
        table.Controls.Add(_stopSourceVirtualMachinesCheckBox, 0, 3);
        table.Controls.Add(_startImportedVirtualMachinesCheckBox, 0, 4);
        table.Controls.Add(_dryRunCheckBox, 0, 5);
        table.Controls.Add(_savePasswordsCheckBox, 0, 6);
        table.Controls.Add(_logPathLabel, 0, 7);

        group.Controls.Add(table);
        return group;
    }

    private Control BuildActionPanel()
    {
        var panel = new TableLayoutPanel();
        panel.Dock = DockStyle.Fill;
        panel.ColumnCount = 1;
        panel.RowCount = 2;
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 34F));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 28F));

        var buttonsPanel = new FlowLayoutPanel();
        buttonsPanel.Dock = DockStyle.Fill;
        buttonsPanel.FlowDirection = FlowDirection.LeftToRight;
        buttonsPanel.WrapContents = false;

        _loadVirtualMachinesButton.Text = "Load VMs";
        _selectAllButton.Text = "Select All";
        _clearSelectionButton.Text = "Clear Selection";
        _loadProxmoxButton.Text = "Load Proxmox";
        _startMigrationButton.Text = "Start VM Migration";

        _loadVirtualMachinesButton.Click += LoadVirtualMachinesButtonClick;
        _selectAllButton.Click += SelectAllButtonClick;
        _clearSelectionButton.Click += ClearSelectionButtonClick;
        _loadProxmoxButton.Click += LoadProxmoxButtonClick;
        _startMigrationButton.Click += StartVirtualMachineMigrationButtonClick;

        buttonsPanel.Controls.Add(_loadVirtualMachinesButton);
        buttonsPanel.Controls.Add(_selectAllButton);
        buttonsPanel.Controls.Add(_clearSelectionButton);
        buttonsPanel.Controls.Add(_loadProxmoxButton);
        buttonsPanel.Controls.Add(_startMigrationButton);

        var statusPanel = new TableLayoutPanel();
        statusPanel.Dock = DockStyle.Fill;
        statusPanel.ColumnCount = 2;
        statusPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 520F));
        statusPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));

        _selectionLabel.Dock = DockStyle.Fill;
        _selectionLabel.TextAlign = ContentAlignment.MiddleLeft;
        _selectionLabel.AutoEllipsis = true;

        _statusLabel.Dock = DockStyle.Fill;
        _statusLabel.TextAlign = ContentAlignment.MiddleLeft;
        _statusLabel.AutoEllipsis = true;

        statusPanel.Controls.Add(_selectionLabel, 0, 0);
        statusPanel.Controls.Add(_statusLabel, 1, 0);

        panel.Controls.Add(buttonsPanel, 0, 0);
        panel.Controls.Add(statusPanel, 0, 1);
        return panel;
    }

    private Control BuildWorkspacePanel()
    {
        _workspaceSplitContainer.Dock = DockStyle.Fill;
        _workspaceSplitContainer.Orientation = Orientation.Horizontal;
        _workspaceSplitContainer.SplitterWidth = 7;
        ConfigureInitialSplitterDistance(_workspaceSplitContainer, 0, 130);

        _workspaceSplitContainer.Panel1.Controls.Add(BuildMainPanel());
        _workspaceSplitContainer.Panel2.Controls.Add(BuildLogPanel());
        return _workspaceSplitContainer;
    }

    private Control BuildMainPanel()
    {
        _mainSplitContainer.Dock = DockStyle.Fill;
        _mainSplitContainer.Orientation = Orientation.Vertical;
        _mainSplitContainer.SplitterWidth = 7;
        ConfigureInitialSplitterDistance(_mainSplitContainer, 760, 0);

        var sourceGroup = new GroupBox();
        sourceGroup.Text = "Synology Virtual Machines";
        sourceGroup.Dock = DockStyle.Fill;

        _sourceVirtualMachineListView.Dock = DockStyle.Fill;
        _sourceVirtualMachineListView.View = View.Details;
        _sourceVirtualMachineListView.CheckBoxes = true;
        _sourceVirtualMachineListView.FullRowSelect = true;
        _sourceVirtualMachineListView.HideSelection = false;
        _sourceVirtualMachineListView.MultiSelect = false;
        _sourceVirtualMachineListView.Columns.Add("Name", 190);
        _sourceVirtualMachineListView.Columns.Add("State", 90);
        _sourceVirtualMachineListView.Columns.Add("CPU", 60);
        _sourceVirtualMachineListView.Columns.Add("Memory (GB)", 90);
        _sourceVirtualMachineListView.Columns.Add("Disks", 60);
        _sourceVirtualMachineListView.Columns.Add("NICs", 60);
        _sourceVirtualMachineListView.Columns.Add("Size (GB)", 90);
        _sourceVirtualMachineListView.SelectedIndexChanged += SourceVirtualMachineSelectionChanged;
        _sourceVirtualMachineListView.ItemChecked += SourceVirtualMachineCheckedChanged;
        sourceGroup.Controls.Add(_sourceVirtualMachineListView);

        var tabs = new TabControl();
        tabs.Dock = DockStyle.Fill;

        var sourceDetailsTab = new TabPage("Source Details");
        sourceDetailsTab.Controls.Add(BuildSourceDetailsPanel());

        var proxmoxTab = new TabPage("Proxmox Target");
        proxmoxTab.Padding = new Padding(0);
        proxmoxTab.Controls.Add(BuildTargetVirtualMachinesPanel());

        tabs.TabPages.Add(sourceDetailsTab);
        tabs.TabPages.Add(proxmoxTab);

        _mainSplitContainer.Panel1.Controls.Add(sourceGroup);
        _mainSplitContainer.Panel2.Controls.Add(tabs);
        return _mainSplitContainer;
    }

    private Control BuildSourceDetailsPanel()
    {
        _sourceDetailsTextBox.Dock = DockStyle.Fill;
        _sourceDetailsTextBox.Multiline = true;
        _sourceDetailsTextBox.ReadOnly = true;
        _sourceDetailsTextBox.ScrollBars = ScrollBars.Both;
        _sourceDetailsTextBox.BorderStyle = BorderStyle.None;
        _sourceDetailsTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _sourceDetailsTextBox.Text = "Select a Synology virtual machine to view CPU, memory, disks, and network adapters.";
        return _sourceDetailsTextBox;
    }

    private Control BuildTargetVirtualMachinesPanel()
    {
        var panel = new Panel();
        panel.Dock = DockStyle.Fill;

        var buttons = new FlowLayoutPanel();
        buttons.Dock = DockStyle.Top;
        buttons.Height = 42;
        buttons.Padding = new Padding(0, 0, 0, 4);
        buttons.FlowDirection = FlowDirection.LeftToRight;
        buttons.WrapContents = false;

        _startTargetVirtualMachineButton.Text = "Start";
        _stopTargetVirtualMachineButton.Text = "Stop";
        _deleteTargetVirtualMachineButton.Text = "Delete";
        _startTargetVirtualMachineButton.Click += StartTargetVirtualMachineButtonClick;
        _stopTargetVirtualMachineButton.Click += StopTargetVirtualMachineButtonClick;
        _deleteTargetVirtualMachineButton.Click += DeleteTargetVirtualMachineButtonClick;

        buttons.Controls.Add(_startTargetVirtualMachineButton);
        buttons.Controls.Add(_stopTargetVirtualMachineButton);
        buttons.Controls.Add(_deleteTargetVirtualMachineButton);

        _targetVirtualMachineListView.Dock = DockStyle.Fill;
        _targetVirtualMachineListView.View = View.Details;
        _targetVirtualMachineListView.FullRowSelect = true;
        _targetVirtualMachineListView.HideSelection = false;
        _targetVirtualMachineListView.MultiSelect = false;
        _targetVirtualMachineListView.Columns.Add("VMID", 70);
        _targetVirtualMachineListView.Columns.Add("Name", 150);
        _targetVirtualMachineListView.Columns.Add("Status", 90);
        _targetVirtualMachineListView.Columns.Add("Cores", 60);
        _targetVirtualMachineListView.Columns.Add("Memory (GB)", 90);
        _targetVirtualMachineListView.Columns.Add("Networks", 180);
        _targetVirtualMachineListView.SelectedIndexChanged += TargetVirtualMachineSelectionChanged;
        _targetVirtualMachineListView.Resize += TargetVirtualMachineListViewResize;

        _targetDetailsTextBox.Dock = DockStyle.Fill;
        _targetDetailsTextBox.Multiline = true;
        _targetDetailsTextBox.ReadOnly = true;
        _targetDetailsTextBox.ScrollBars = ScrollBars.Both;
        _targetDetailsTextBox.BorderStyle = BorderStyle.None;
        _targetDetailsTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _targetDetailsTextBox.Text = "Load Proxmox inventory to manage imported virtual machines.";

        _targetVirtualMachineSplitContainer.Dock = DockStyle.Fill;
        _targetVirtualMachineSplitContainer.Orientation = Orientation.Horizontal;
        _targetVirtualMachineSplitContainer.SplitterWidth = 6;
        ConfigureInitialSplitterDistance(_targetVirtualMachineSplitContainer, 0, 150);
        _targetVirtualMachineSplitContainer.Panel1.Controls.Add(_targetVirtualMachineListView);
        _targetVirtualMachineSplitContainer.Panel2.Controls.Add(_targetDetailsTextBox);

        panel.Controls.Add(_targetVirtualMachineSplitContainer);
        panel.Controls.Add(buttons);
        return panel;
    }

    private Control BuildLogPanel()
    {
        var group = new GroupBox();
        group.Text = "Process Log";
        group.Dock = DockStyle.Fill;

        _logTextBox.Dock = DockStyle.Fill;
        _logTextBox.ReadOnly = true;
        _logTextBox.BackColor = SurfaceColor;
        _logTextBox.BorderStyle = BorderStyle.None;
        _logTextBox.Font = new Font(FontFamily.GenericMonospace, 9F);
        _logTextBox.ScrollBars = RichTextBoxScrollBars.Vertical;

        group.Controls.Add(_logTextBox);
        return group;
    }

    private void AddField(TableLayoutPanel table, int rowIndex, string labelText, Control control)
    {
        table.Controls.Add(BuildLabel(labelText), 0, rowIndex);
        control.Dock = DockStyle.Fill;
        if (control is TextBox || control is ComboBox)
        {
            control.Margin = new Padding(3, 4, 3, 4);
        }

        table.Controls.Add(control, 1, rowIndex);
    }

    private static Label BuildLabel(string text)
    {
        var label = new Label();
        label.Dock = DockStyle.Fill;
        label.AutoSize = false;
        label.TextAlign = ContentAlignment.MiddleLeft;
        label.Margin = new Padding(3, 4, 3, 4);
        label.Text = text;
        return label;
    }

    private static void ConfigureInitialSplitterDistance(SplitContainer split, int preferredDistance, int preferredPanel2Size)
    {
        split.Panel1MinSize = 150;
        split.Panel2MinSize = 140;
        split.Resize += delegate
        {
            var available = split.Orientation == Orientation.Vertical ? split.ClientSize.Width : split.ClientSize.Height;
            if (available <= 0)
            {
                return;
            }

            var desired = preferredDistance > 0 ? preferredDistance : available - preferredPanel2Size - split.SplitterWidth;
            var maxDistance = available - split.Panel2MinSize - split.SplitterWidth;
            if (maxDistance < split.Panel1MinSize)
            {
                return;
            }

            if (split.SplitterDistance <= 0 || split.SplitterDistance > maxDistance)
            {
                split.SplitterDistance = Math.Max(split.Panel1MinSize, Math.Min(maxDistance, desired));
            }
        };
    }

    private void ApplyTheme()
    {
        ApplyButtonTheme(_containersScreenButton, Color.FromArgb(224, 238, 241), AccentDarkColor);
        ApplyButtonTheme(_loadVirtualMachinesButton, AccentDarkColor, Color.White);
        ApplyButtonTheme(_selectAllButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_clearSelectionButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_loadProxmoxButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_startMigrationButton, AccentColor, Color.White);
        ApplyButtonTheme(_startTargetVirtualMachineButton, AccentColor, Color.White);
        ApplyButtonTheme(_stopTargetVirtualMachineButton, SecondaryButtonColor, TextColor);
        ApplyButtonTheme(_deleteTargetVirtualMachineButton, Color.FromArgb(174, 76, 60), Color.White);

        ApplySurfaceTheme(this);
    }

    private void ApplySurfaceTheme(Control control)
    {
        foreach (Control child in control.Controls)
        {
            if (child is GroupBox)
            {
                child.BackColor = SurfaceColor;
                child.ForeColor = TextColor;
            }
            else if (child is TabPage)
            {
                child.BackColor = SurfaceColor;
                child.ForeColor = TextColor;
            }
            else if (child is RichTextBox || child is TextBox || child is ComboBox || child is ListView)
            {
                child.BackColor = SurfaceColor;
                child.ForeColor = TextColor;
            }
            else
            {
                child.ForeColor = TextColor;
            }

            ApplySurfaceTheme(child);
        }
    }

    private static void ApplyButtonTheme(Button button, Color backColor, Color foreColor)
    {
        button.BackColor = backColor;
        button.ForeColor = foreColor;
        button.FlatStyle = FlatStyle.Flat;
        button.FlatAppearance.BorderSize = 0;
        button.AutoSize = false;
        button.Height = 32;
        button.Width = Math.Max(button.Width, 120);
        button.TextAlign = ContentAlignment.MiddleCenter;
        button.Font = new Font("Segoe UI Semibold", 9F, FontStyle.Bold, GraphicsUnit.Point);
    }

    private void InitializeDefaults()
    {
        if (string.IsNullOrWhiteSpace(_sourcePortTextBox.Text))
        {
            _sourcePortTextBox.Text = "22";
        }

        if (string.IsNullOrWhiteSpace(_targetPortTextBox.Text))
        {
            _targetPortTextBox.Text = "22";
        }

        if (string.IsNullOrWhiteSpace(_targetRootTextBox.Text))
        {
            _targetRootTextBox.Text = MigratorCore.DefaultVirtualMachineTargetRoot;
        }

        UpdateRunButtonText();
    }

    private void InitializeLogger()
    {
        var baseDir = AppDomain.CurrentDomain.BaseDirectory;
        var logsDir = Path.Combine(baseDir, "logs");
        var fileName = "vm-session-" + DateTime.Now.ToString("yyyyMMdd-HHmmss") + ".log";
        _logger = new SessionLogger(Path.Combine(logsDir, fileName));
        _logPathLabel.Text = _logger.Path;
        AppendLogLine("Session started. Version: " + Program.AppVersion);
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

            ApplySavedText(_sourceHostTextBox, profile.VmSourceHost);
            ApplySavedText(_sourcePortTextBox, profile.VmSourcePort);
            ApplySavedText(_sourceUserTextBox, profile.VmSourceLogin);
            ApplySavedText(_targetHostTextBox, profile.VmTargetHost);
            ApplySavedText(_targetPortTextBox, profile.VmTargetPort);
            ApplySavedText(_targetUserTextBox, profile.VmTargetLogin);
            _savePasswordsCheckBox.Checked = profile.VmSavePasswords;
            if (profile.VmSavePasswords)
            {
                ApplySavedText(_sourcePasswordTextBox, profile.VmSourcePassword);
                ApplySavedText(_targetPasswordTextBox, profile.VmTargetPassword);
            }
            ApplySavedText(_targetRootTextBox, profile.VmTargetRoot);
            ApplySavedText(_targetStorageComboBox, profile.VmTargetStorage);
            ApplySavedText(_targetBridgeComboBox, profile.VmTargetBridge);
            _stopSourceVirtualMachinesCheckBox.Checked = profile.VmStopSourceDuringExport;
            _startImportedVirtualMachinesCheckBox.Checked = profile.VmStartImportedVirtualMachines;
            RestoreWindowLayout(profile);
            _pendingLayoutProfile = profile;
            AppendLogLine("Saved VM profile loaded.");
        }
        catch (Exception ex)
        {
            AppendLogLine("[!] Failed to load saved VM profile: " + ex.Message);
        }
    }

    private void SaveConnectionProfile()
    {
        try
        {
            ConnectionProfile profile = null;
            if (File.Exists(_profilePath))
            {
                var existingJson = File.ReadAllText(_profilePath, Encoding.UTF8);
                profile = Json.Deserialize<ConnectionProfile>(existingJson);
            }

            if (profile == null)
            {
                profile = new ConnectionProfile();
            }

            profile.VmSourceHost = NormalizeSavedValue(_sourceHostTextBox.Text);
            profile.VmSourcePort = NormalizeSavedValue(_sourcePortTextBox.Text);
            profile.VmSourceLogin = NormalizeSavedValue(_sourceUserTextBox.Text);
            profile.VmTargetHost = NormalizeSavedValue(_targetHostTextBox.Text);
            profile.VmTargetPort = NormalizeSavedValue(_targetPortTextBox.Text);
            profile.VmTargetLogin = NormalizeSavedValue(_targetUserTextBox.Text);
            profile.VmSavePasswords = _savePasswordsCheckBox.Checked;
            profile.VmSourcePassword = _savePasswordsCheckBox.Checked ? NormalizeSavedValue(_sourcePasswordTextBox.Text) : null;
            profile.VmTargetPassword = _savePasswordsCheckBox.Checked ? NormalizeSavedValue(_targetPasswordTextBox.Text) : null;
            profile.VmTargetRoot = NormalizeSavedValue(_targetRootTextBox.Text) ?? MigratorCore.DefaultVirtualMachineTargetRoot;
            profile.VmTargetStorage = NormalizeSavedValue(_targetStorageComboBox.Text);
            profile.VmTargetBridge = NormalizeSavedValue(_targetBridgeComboBox.Text);
            profile.VmStopSourceDuringExport = _stopSourceVirtualMachinesCheckBox.Checked;
            profile.VmStartImportedVirtualMachines = _startImportedVirtualMachinesCheckBox.Checked;

            var restoreBounds = WindowState == FormWindowState.Normal ? Bounds : RestoreBounds;
            profile.VmWindowLeft = restoreBounds.Left;
            profile.VmWindowTop = restoreBounds.Top;
            profile.VmWindowWidth = restoreBounds.Width;
            profile.VmWindowHeight = restoreBounds.Height;
            profile.VmWindowState = WindowState == FormWindowState.Maximized ? "Maximized" : "Normal";
            profile.VmWorkspaceSplitterDistance = CaptureSplitterDistance(_workspaceSplitContainer);
            profile.VmMainSplitterDistance = CaptureSplitterDistance(_mainSplitContainer);
            profile.VmTargetSplitterDistance = CaptureSplitterDistance(_targetVirtualMachineSplitContainer);
            profile.VmSourceVirtualMachineColumnWidths = CaptureColumnWidths(_sourceVirtualMachineListView);
            profile.VmTargetVirtualMachineColumnWidths = CaptureColumnWidths(_targetVirtualMachineListView);

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

    private void RestoreWindowLayout(ConnectionProfile profile)
    {
        if (profile == null || !profile.VmWindowWidth.HasValue || !profile.VmWindowHeight.HasValue)
        {
            return;
        }

        var bounds = new Rectangle(
            profile.VmWindowLeft ?? Left,
            profile.VmWindowTop ?? Top,
            profile.VmWindowWidth.Value,
            profile.VmWindowHeight.Value);

        if (!IsUsableWindowBounds(bounds))
        {
            return;
        }

        StartPosition = FormStartPosition.Manual;
        DesktopBounds = bounds;
        if (string.Equals(profile.VmWindowState, "Maximized", StringComparison.OrdinalIgnoreCase))
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
            ApplyColumnWidths(_sourceVirtualMachineListView, profile.VmSourceVirtualMachineColumnWidths);
            ApplyColumnWidths(_targetVirtualMachineListView, profile.VmTargetVirtualMachineColumnWidths);
            ApplySplitterDistance(_workspaceSplitContainer, profile.VmWorkspaceSplitterDistance);
            ApplySplitterDistance(_mainSplitContainer, profile.VmMainSplitterDistance);
            ApplySplitterDistance(_targetVirtualMachineSplitContainer, profile.VmTargetSplitterDistance);
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

        var available = split.Orientation == Orientation.Vertical ? split.ClientSize.Width : split.ClientSize.Height;
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

    private void TargetVirtualMachineListViewResize(object sender, EventArgs e)
    {
        AdjustTargetVirtualMachineColumnsToWidth();
    }

    private void AdjustTargetVirtualMachineColumnsToWidth()
    {
        if (_targetVirtualMachineListView.Columns.Count < 6)
        {
            return;
        }

        var clientWidth = _targetVirtualMachineListView.ClientSize.Width;
        if (clientWidth <= 0)
        {
            return;
        }

        var fixedWidth =
            _targetVirtualMachineListView.Columns[0].Width +
            _targetVirtualMachineListView.Columns[1].Width +
            _targetVirtualMachineListView.Columns[2].Width +
            _targetVirtualMachineListView.Columns[3].Width +
            _targetVirtualMachineListView.Columns[4].Width;

        var reserved = fixedWidth + SystemInformation.VerticalScrollBarWidth + 8;
        var fillWidth = Math.Max(180, clientWidth - reserved);
        _targetVirtualMachineListView.Columns[5].Width = fillWidth;
    }

    private void ContainersScreenButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        SaveConnectionProfile();
        Close();
    }

    private void VmMigrationFormShown(object sender, EventArgs e)
    {
        ApplySavedLayoutProfile();
        BeginInvoke((Action)AdjustTargetVirtualMachineColumnsToWidth);
    }

    private void VmMigrationFormClosing(object sender, FormClosingEventArgs e)
    {
        SaveConnectionProfile();
    }

    private void VmMigrationFormClosed(object sender, FormClosedEventArgs e)
    {
        if (_containersForm != null && !_containersForm.IsDisposed)
        {
            _containersForm.Show();
            _containersForm.BringToFront();
        }
    }

    private void DryRunCheckBoxChanged(object sender, EventArgs e)
    {
        UpdateRunButtonText();
        if (!_busy)
        {
            _statusLabel.Text = _dryRunCheckBox.Checked
                ? "Dry Run enabled. The app will validate and build a VM migration plan only."
                : "Ready.";
        }
    }

    private void LoadVirtualMachinesButtonClick(object sender, EventArgs e)
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

        SaveConnectionProfile();
        List<VirtualMachineDefinition> definitions = null;
        List<string> targetStorages = null;
        List<string> targetBridges = null;
        List<ProxmoxVirtualMachineDefinition> targetDefinitions = null;
        string targetInventoryError = null;
        RunWorker(
            "Loading virtual machines from Synology...",
            delegate
            {
                definitions = MigratorCore.DiscoverVirtualMachines(source);
                ConnectionInfoData target;
                if (TryBuildTargetConnectionInfo(out target))
                {
                    try
                    {
                        targetStorages = MigratorCore.DiscoverProxmoxStorages(target);
                        targetBridges = MigratorCore.DiscoverProxmoxBridges(target);
                        targetDefinitions = MigratorCore.DiscoverProxmoxVirtualMachines(target);
                    }
                    catch (Exception ex)
                    {
                        targetInventoryError = ex.Message;
                    }
                }
            },
            delegate
            {
                UpdateSourceVirtualMachineInventory(definitions);
                if (targetStorages != null || targetBridges != null || targetDefinitions != null)
                {
                    UpdateTargetResources(targetStorages ?? new List<string>(), targetBridges ?? new List<string>());
                    UpdateTargetVirtualMachineInventory(targetDefinitions ?? new List<ProxmoxVirtualMachineDefinition>());
                    AppendLogLine("Loaded Proxmox storages: " + ((targetStorages == null || targetStorages.Count == 0) ? "none" : string.Join(", ", targetStorages)));
                    AppendLogLine("Loaded Proxmox bridges: " + ((targetBridges == null || targetBridges.Count == 0) ? "none" : string.Join(", ", targetBridges)));
                    AppendLogLine("Loaded Proxmox VMs: " + ((targetDefinitions == null || targetDefinitions.Count == 0) ? "none" : string.Join(", ", targetDefinitions.Select(item => item.Name))));
                }
                else if (!string.IsNullOrWhiteSpace(targetInventoryError))
                {
                    AppendLogLine("[!] Proxmox inventory was not loaded automatically: " + targetInventoryError);
                }

                AppendLogLine("Loaded " + definitions.Count.ToString(CultureInfo.InvariantCulture) + " Synology virtual machine(s).");
            });
    }

    private void LoadProxmoxButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To load Proxmox inventory, fill in Host, Port, Login, and Password in the Proxmox Target section.");
            return;
        }

        SaveConnectionProfile();
        RefreshProxmoxInventory(target, "Loading Proxmox inventory...");
    }

    private void RefreshProxmoxInventory(ConnectionInfoData target, string busyText)
    {
        List<string> storages = null;
        List<string> bridges = null;
        List<ProxmoxVirtualMachineDefinition> definitions = null;

        RunWorker(
            busyText,
            delegate
            {
                storages = MigratorCore.DiscoverProxmoxStorages(target);
                bridges = MigratorCore.DiscoverProxmoxBridges(target);
                definitions = MigratorCore.DiscoverProxmoxVirtualMachines(target);
            },
            delegate
            {
                UpdateTargetResources(storages, bridges);
                UpdateTargetVirtualMachineInventory(definitions);
                AppendLogLine("Loaded Proxmox storages: " + (storages.Count == 0 ? "none" : string.Join(", ", storages)));
                AppendLogLine("Loaded Proxmox bridges: " + (bridges.Count == 0 ? "none" : string.Join(", ", bridges)));
                AppendLogLine("Loaded Proxmox VMs: " + (definitions.Count == 0 ? "none" : string.Join(", ", definitions.Select(item => item.Name))));
            });
    }

    private void SelectAllButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        _suppressSourceItemCheckedEvents = true;
        foreach (ListViewItem item in _sourceVirtualMachineListView.Items)
        {
            item.Checked = true;
        }

        _suppressSourceItemCheckedEvents = false;
        UpdateSelectionSummary();
        UpdateUiState();
    }

    private void ClearSelectionButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        _suppressSourceItemCheckedEvents = true;
        foreach (ListViewItem item in _sourceVirtualMachineListView.Items)
        {
            item.Checked = false;
        }

        _suppressSourceItemCheckedEvents = false;
        UpdateSelectionSummary();
        UpdateUiState();
    }

    private void StartVirtualMachineMigrationButtonClick(object sender, EventArgs e)
    {
        if (_busy)
        {
            return;
        }

        if (_virtualMachines.Count == 0)
        {
            ShowError("Load virtual machines from Synology first.");
            return;
        }

        var selected = GetCheckedVirtualMachines().ToList();
        if (selected.Count == 0)
        {
            ShowError("Select at least one virtual machine for migration.");
            return;
        }

        ConnectionInfoData source;
        ConnectionInfoData target;
        try
        {
            source = BuildConnectionInfo(_sourceHostTextBox, _sourcePortTextBox, _sourceUserTextBox, _sourcePasswordTextBox, "Synology");
            target = BuildConnectionInfo(_targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox, "Proxmox");
        }
        catch (Exception ex)
        {
            ShowError(ex.Message);
            return;
        }

        var targetStorage = NormalizeSavedValue(_targetStorageComboBox.Text);
        if (string.IsNullOrWhiteSpace(targetStorage))
        {
            ShowError("Select or enter a Proxmox target storage.");
            return;
        }

        var targetBridge = NormalizeSavedValue(_targetBridgeComboBox.Text);
        if (string.IsNullOrWhiteSpace(targetBridge))
        {
            ShowError("Select or enter a Proxmox bridge.");
            return;
        }

        var options = new VirtualMachineMigrationOptions
        {
            Source = source,
            Target = target,
            VirtualMachineNames = selected.Select(item => item.Definition.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
            TargetRoot = NormalizeSavedValue(_targetRootTextBox.Text) ?? MigratorCore.DefaultVirtualMachineTargetRoot,
            TargetStorage = targetStorage,
            TargetBridge = targetBridge,
            StopVirtualMachinesDuringExport = _stopSourceVirtualMachinesCheckBox.Checked,
            StartImportedVirtualMachines = _startImportedVirtualMachinesCheckBox.Checked,
            DryRun = _dryRunCheckBox.Checked
        };

        SaveConnectionProfile();
        List<ProxmoxVirtualMachineDefinition> refreshedTargetDefinitions = null;
        List<string> refreshedStorages = null;
        List<string> refreshedBridges = null;

        RunWorker(
            _dryRunCheckBox.Checked ? "Building VM dry run plan..." : "Migrating selected virtual machines...",
            delegate
            {
                MigratorCore.RunVirtualMachineMigration(options);
                if (!options.DryRun)
                {
                    refreshedStorages = MigratorCore.DiscoverProxmoxStorages(target);
                    refreshedBridges = MigratorCore.DiscoverProxmoxBridges(target);
                    refreshedTargetDefinitions = MigratorCore.DiscoverProxmoxVirtualMachines(target);
                }
            },
            delegate
            {
                if (_dryRunCheckBox.Checked)
                {
                    AppendLogLine("VM dry run plan completed. No disks were copied and no changes were applied.");
                    MessageBox.Show(this, "VM Dry Run plan has been built. Review the process log for the planned steps.", "Dry Run Ready", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    return;
                }

                UpdateTargetResources(refreshedStorages ?? new List<string>(), refreshedBridges ?? new List<string>());
                UpdateTargetVirtualMachineInventory(refreshedTargetDefinitions ?? new List<ProxmoxVirtualMachineDefinition>());
                AppendLogLine("VM migration completed. Import root on target: " + options.TargetRoot);
                MessageBox.Show(this, "Virtual machine migration completed successfully.", "Done", MessageBoxButtons.OK, MessageBoxIcon.Information);
            });
    }

    private void StartTargetVirtualMachineButtonClick(object sender, EventArgs e)
    {
        ManageTargetVirtualMachine(
            "start",
            "Starting Proxmox VM...",
            delegate(ConnectionInfoData target, int vmId) { MigratorCore.StartTargetVirtualMachine(target, vmId); });
    }

    private void StopTargetVirtualMachineButtonClick(object sender, EventArgs e)
    {
        ManageTargetVirtualMachine(
            "stop",
            "Stopping Proxmox VM...",
            delegate(ConnectionInfoData target, int vmId) { MigratorCore.StopTargetVirtualMachine(target, vmId); });
    }

    private void DeleteTargetVirtualMachineButtonClick(object sender, EventArgs e)
    {
        var definition = GetSelectedTargetVirtualMachine();
        if (definition == null)
        {
            ShowError("Select a Proxmox virtual machine first.");
            return;
        }

        var decision = MessageBox.Show(
            this,
            "Delete Proxmox VM \"" + definition.Name + "\" (VMID " + definition.VmId.ToString(CultureInfo.InvariantCulture) + ")?" + Environment.NewLine + Environment.NewLine +
            "This will run qm destroy with purge and destroy-unreferenced-disks enabled.",
            "Delete Proxmox VM",
            MessageBoxButtons.YesNo,
            MessageBoxIcon.Warning);

        if (decision != DialogResult.Yes)
        {
            return;
        }

        ManageTargetVirtualMachine(
            "delete",
            "Deleting Proxmox VM...",
            delegate(ConnectionInfoData target, int vmId) { MigratorCore.DeleteTargetVirtualMachine(target, vmId); });
    }

    private void ManageTargetVirtualMachine(string actionName, string busyText, Action<ConnectionInfoData, int> action)
    {
        if (_busy)
        {
            return;
        }

        var definition = GetSelectedTargetVirtualMachine();
        if (definition == null)
        {
            ShowError("Select a Proxmox virtual machine first.");
            return;
        }

        ConnectionInfoData target;
        if (!TryBuildTargetConnectionInfo(out target))
        {
            ShowError("To manage Proxmox VMs, fill in Host, Port, Login, and Password in the Proxmox Target section.");
            return;
        }

        List<ProxmoxVirtualMachineDefinition> refreshedDefinitions = null;
        List<string> refreshedStorages = null;
        List<string> refreshedBridges = null;
        var vmId = definition.VmId;
        RunWorker(
            busyText,
            delegate
            {
                action(target, vmId);
                refreshedStorages = MigratorCore.DiscoverProxmoxStorages(target);
                refreshedBridges = MigratorCore.DiscoverProxmoxBridges(target);
                refreshedDefinitions = MigratorCore.DiscoverProxmoxVirtualMachines(target);
            },
            delegate
            {
                UpdateTargetResources(refreshedStorages ?? new List<string>(), refreshedBridges ?? new List<string>());
                UpdateTargetVirtualMachineInventory(refreshedDefinitions ?? new List<ProxmoxVirtualMachineDefinition>());
                AppendLogLine("Target VM action completed: " + actionName + " VMID " + vmId.ToString(CultureInfo.InvariantCulture));
            });
    }

    private void UpdateSourceVirtualMachineInventory(List<VirtualMachineDefinition> definitions)
    {
        var selected = GetSelectedSourceVirtualMachine();
        var selectedName = selected == null ? null : selected.Definition.Name;
        var checkedNames = new HashSet<string>(GetCheckedVirtualMachines().Select(item => item.Definition.Name), StringComparer.OrdinalIgnoreCase);

        _virtualMachines.Clear();
        _virtualMachines.AddRange((definitions ?? new List<VirtualMachineDefinition>())
            .Where(item => item != null)
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .Select(item => new VirtualMachineViewModel { Definition = item }));

        _suppressSourceItemCheckedEvents = true;
        _sourceVirtualMachineListView.BeginUpdate();
        _sourceVirtualMachineListView.Items.Clear();
        foreach (var model in _virtualMachines)
        {
            var definition = model.Definition;
            var item = new ListViewItem(VmGuiFormatter.GetDisplayName(definition));
            item.Tag = model;
            item.Checked = checkedNames.Contains(definition.Name);
            item.SubItems.Add(string.IsNullOrWhiteSpace(definition.State) ? "unknown" : definition.State);
            item.SubItems.Add(definition.Vcpus.ToString(CultureInfo.InvariantCulture));
            item.SubItems.Add(VmGuiFormatter.FormatGiBFromKiB(definition.MemoryKiB));
            item.SubItems.Add(definition.Disks.Count.ToString(CultureInfo.InvariantCulture));
            item.SubItems.Add(definition.Interfaces.Count.ToString(CultureInfo.InvariantCulture));
            item.SubItems.Add(VmGuiFormatter.FormatGiB(definition.EstimatedTransferBytes));
            _sourceVirtualMachineListView.Items.Add(item);
        }

        _sourceVirtualMachineListView.EndUpdate();
        _suppressSourceItemCheckedEvents = false;
        SelectSourceVirtualMachineByName(selectedName);
        if (_sourceVirtualMachineListView.SelectedItems.Count == 0 && _sourceVirtualMachineListView.Items.Count > 0)
        {
            _sourceVirtualMachineListView.Items[0].Selected = true;
        }

        if (_sourceVirtualMachineListView.SelectedItems.Count == 0)
        {
            _sourceDetailsTextBox.Text = "Select a Synology virtual machine to view CPU, memory, disks, and network adapters.";
        }

        UpdateSelectionSummary();
        UpdateUiState();
    }

    private void UpdateTargetVirtualMachineInventory(List<ProxmoxVirtualMachineDefinition> definitions)
    {
        var selected = GetSelectedTargetVirtualMachine();
        var selectedVmId = selected == null ? (int?)null : selected.VmId;

        _targetVirtualMachines.Clear();
        _targetVirtualMachines.AddRange((definitions ?? new List<ProxmoxVirtualMachineDefinition>())
            .Where(item => item != null)
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.VmId));

        _targetVirtualMachineListView.BeginUpdate();
        _targetVirtualMachineListView.Items.Clear();
        foreach (var definition in _targetVirtualMachines)
        {
            var item = new ListViewItem(definition.VmId.ToString(CultureInfo.InvariantCulture));
            item.Tag = definition;
            item.SubItems.Add(definition.Name ?? string.Empty);
            item.SubItems.Add(definition.Status ?? string.Empty);
            item.SubItems.Add(definition.Cores.ToString(CultureInfo.InvariantCulture));
            item.SubItems.Add(VmGuiFormatter.FormatGiBFromMb(definition.MemoryMb));
            item.SubItems.Add(VmGuiFormatter.BuildNetworksSummary(definition));
            _targetVirtualMachineListView.Items.Add(item);
        }

        _targetVirtualMachineListView.EndUpdate();
        SelectTargetVirtualMachineById(selectedVmId);
        if (_targetVirtualMachineListView.SelectedItems.Count == 0 && _targetVirtualMachineListView.Items.Count > 0)
        {
            _targetVirtualMachineListView.Items[0].Selected = true;
        }

        if (_targetVirtualMachineListView.SelectedItems.Count == 0)
        {
            _targetDetailsTextBox.Text = "Load Proxmox inventory to manage imported virtual machines.";
        }

        UpdateUiState();
    }

    private void UpdateTargetResources(List<string> storages, List<string> bridges)
    {
        var selectedStorage = NormalizeSavedValue(_targetStorageComboBox.Text);
        var selectedBridge = NormalizeSavedValue(_targetBridgeComboBox.Text);

        _targetStorages.Clear();
        _targetBridges.Clear();
        if (storages != null)
        {
            _targetStorages.AddRange(storages.Where(item => !string.IsNullOrWhiteSpace(item)).Distinct(StringComparer.OrdinalIgnoreCase));
        }

        if (bridges != null)
        {
            _targetBridges.AddRange(bridges.Where(item => !string.IsNullOrWhiteSpace(item)).Distinct(StringComparer.OrdinalIgnoreCase));
        }

        UpdateComboBoxItems(_targetStorageComboBox, _targetStorages, selectedStorage);
        UpdateComboBoxItems(_targetBridgeComboBox, _targetBridges, selectedBridge);
        UpdateUiState();
    }

    private static void UpdateComboBoxItems(ComboBox comboBox, List<string> values, string preferredValue)
    {
        comboBox.BeginUpdate();
        comboBox.Items.Clear();
        foreach (var value in values.OrderBy(item => item, StringComparer.OrdinalIgnoreCase))
        {
            comboBox.Items.Add(value);
        }

        comboBox.EndUpdate();
        if (!string.IsNullOrWhiteSpace(preferredValue))
        {
            comboBox.Text = preferredValue;
        }
        else if (comboBox.Items.Count > 0 && string.IsNullOrWhiteSpace(comboBox.Text))
        {
            comboBox.SelectedIndex = 0;
        }
    }

    private void SelectSourceVirtualMachineByName(string vmName)
    {
        if (string.IsNullOrWhiteSpace(vmName))
        {
            return;
        }

        foreach (ListViewItem item in _sourceVirtualMachineListView.Items)
        {
            var model = item.Tag as VirtualMachineViewModel;
            if (model == null || !string.Equals(model.Definition.Name, vmName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            item.Selected = true;
            item.Focused = true;
            item.EnsureVisible();
            return;
        }
    }

    private void SelectTargetVirtualMachineById(int? vmId)
    {
        if (!vmId.HasValue)
        {
            return;
        }

        foreach (ListViewItem item in _targetVirtualMachineListView.Items)
        {
            var definition = item.Tag as ProxmoxVirtualMachineDefinition;
            if (definition == null || definition.VmId != vmId.Value)
            {
                continue;
            }

            item.Selected = true;
            item.Focused = true;
            item.EnsureVisible();
            return;
        }
    }

    private VirtualMachineViewModel GetSelectedSourceVirtualMachine()
    {
        return _sourceVirtualMachineListView.SelectedItems.Count == 0
            ? null
            : _sourceVirtualMachineListView.SelectedItems[0].Tag as VirtualMachineViewModel;
    }

    private IEnumerable<VirtualMachineViewModel> GetCheckedVirtualMachines()
    {
        foreach (ListViewItem item in _sourceVirtualMachineListView.Items)
        {
            if (!item.Checked)
            {
                continue;
            }

            var model = item.Tag as VirtualMachineViewModel;
            if (model != null)
            {
                yield return model;
            }
        }
    }

    private ProxmoxVirtualMachineDefinition GetSelectedTargetVirtualMachine()
    {
        return _targetVirtualMachineListView.SelectedItems.Count == 0
            ? null
            : _targetVirtualMachineListView.SelectedItems[0].Tag as ProxmoxVirtualMachineDefinition;
    }

    private void SourceVirtualMachineSelectionChanged(object sender, EventArgs e)
    {
        var selected = GetSelectedSourceVirtualMachine();
        _sourceDetailsTextBox.Text = selected == null
            ? "Select a Synology virtual machine to view CPU, memory, disks, and network adapters."
            : VmGuiFormatter.BuildSourceDetails(selected.Definition);
        UpdateUiState();
    }

    private void SourceVirtualMachineCheckedChanged(object sender, ItemCheckedEventArgs e)
    {
        if (_suppressSourceItemCheckedEvents)
        {
            return;
        }

        UpdateSelectionSummary();
        UpdateUiState();
    }

    private void TargetVirtualMachineSelectionChanged(object sender, EventArgs e)
    {
        var selected = GetSelectedTargetVirtualMachine();
        _targetDetailsTextBox.Text = selected == null
            ? "Load Proxmox inventory to manage imported virtual machines."
            : VmGuiFormatter.BuildTargetDetails(selected);
        UpdateUiState();
    }

    private void UpdateSelectionSummary()
    {
        var selected = GetCheckedVirtualMachines().ToList();
        var total = _virtualMachines.Count;
        var selectedSize = selected.Sum(item => item.Definition == null ? 0L : item.Definition.EstimatedTransferBytes);

        _selectionLabel.Text = "Selected: " + selected.Count.ToString(CultureInfo.InvariantCulture) +
                               " / " + total.ToString(CultureInfo.InvariantCulture) +
                               " | Size: " + VmGuiFormatter.FormatGiB(selectedSize) + " GB";
    }

    private void UpdateRunButtonText()
    {
        _startMigrationButton.Text = _dryRunCheckBox.Checked ? "Build VM Dry Run Plan" : "Start VM Migration";
        _startMigrationButton.BackColor = _dryRunCheckBox.Checked ? Color.FromArgb(186, 120, 33) : AccentColor;
    }

    private void UpdateUiState()
    {
        var hasSourceInventory = _virtualMachines.Count > 0;
        var selectedTarget = GetSelectedTargetVirtualMachine();
        var canLoadProxmox = !_busy && CanLoadTargetConnection();

        _loadVirtualMachinesButton.Enabled = !_busy;
        _selectAllButton.Enabled = !_busy && hasSourceInventory;
        _clearSelectionButton.Enabled = !_busy && _sourceVirtualMachineListView.CheckedItems.Count > 0;
        _loadProxmoxButton.Enabled = canLoadProxmox;
        _startMigrationButton.Enabled = !_busy && _sourceVirtualMachineListView.CheckedItems.Count > 0;
        _containersScreenButton.Enabled = !_busy;
        _sourceVirtualMachineListView.Enabled = !_busy;
        _targetVirtualMachineListView.Enabled = !_busy;
        _targetStorageComboBox.Enabled = !_busy;
        _targetBridgeComboBox.Enabled = !_busy;
        _stopSourceVirtualMachinesCheckBox.Enabled = !_busy;
        _startImportedVirtualMachinesCheckBox.Enabled = !_busy;
        _dryRunCheckBox.Enabled = !_busy;
        _startTargetVirtualMachineButton.Enabled = canLoadProxmox && selectedTarget != null && !selectedTarget.Running;
        _stopTargetVirtualMachineButton.Enabled = canLoadProxmox && selectedTarget != null && selectedTarget.Running;
        _deleteTargetVirtualMachineButton.Enabled = canLoadProxmox && selectedTarget != null;
        if (!_busy && string.IsNullOrWhiteSpace(_statusLabel.Text))
        {
            _statusLabel.Text = "Ready.";
        }
    }

    private bool CanLoadTargetConnection()
    {
        ConnectionInfoData target;
        return TryBuildTargetConnectionInfo(out target);
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
        _statusLabel.Text = statusText;
        UpdateUiState();
    }

    private void HandleCoreLog(string message)
    {
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
        _statusLabel.Text = line;
    }

    private bool TryBuildTargetConnectionInfo(out ConnectionInfoData target)
    {
        try
        {
            target = BuildConnectionInfo(_targetHostTextBox, _targetPortTextBox, _targetUserTextBox, _targetPasswordTextBox, "Proxmox");
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
        var host = NormalizeSavedValue(hostTextBox.Text);
        var portText = NormalizeSavedValue(portTextBox.Text);
        var user = NormalizeSavedValue(userTextBox.Text);
        var passwordText = passwordTextBox.Text ?? string.Empty;

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new InvalidOperationException(sideName + " host is required.");
        }

        int port;
        if (!int.TryParse(portText, NumberStyles.Integer, CultureInfo.InvariantCulture, out port) || port <= 0 || port > 65535)
        {
            throw new InvalidOperationException(sideName + " SSH port is invalid.");
        }

        if (string.IsNullOrWhiteSpace(user))
        {
            throw new InvalidOperationException(sideName + " login is required.");
        }

        if (string.IsNullOrWhiteSpace(passwordText))
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
        var secure = new SecureString();
        foreach (var character in value ?? string.Empty)
        {
            secure.AppendChar(character);
        }

        secure.MakeReadOnly();
        return secure;
    }

    private static void ApplySavedText(TextBox textBox, string value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            textBox.Text = value.Trim();
        }
    }

    private static void ApplySavedText(ComboBox comboBox, string value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            comboBox.Text = value.Trim();
        }
    }

    private static string NormalizeSavedValue(string value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private void ShowError(string message)
    {
        MessageBox.Show(this, message, "Virtual Machine Migration", MessageBoxButtons.OK, MessageBoxIcon.Error);
    }
}

internal static class VmGuiFormatter
{
    internal static string GetDisplayName(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        return string.IsNullOrWhiteSpace(definition.DisplayName) ? (definition.Name ?? string.Empty) : definition.DisplayName;
    }

    internal static string BuildSourceDetails(VirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        AppendLine(builder, "Name", GetDisplayName(definition));
        if (!string.IsNullOrWhiteSpace(definition.Name) &&
            !string.Equals(definition.Name, GetDisplayName(definition), StringComparison.OrdinalIgnoreCase))
        {
            AppendLine(builder, "Identifier", definition.Name);
        }
        AppendLine(builder, "UUID", definition.Uuid);
        AppendLine(builder, "State", definition.State);
        AppendLine(builder, "Autostart", definition.Autostart ? "yes" : "no");
        AppendLine(builder, "CPU", definition.Vcpus.ToString(CultureInfo.InvariantCulture));
        AppendLine(builder, "Memory", FormatGiBFromKiB(definition.MemoryKiB) + " GB");
        AppendLine(builder, "OS type", definition.OsType);
        AppendLine(builder, "Machine type", definition.MachineType);
        AppendLine(builder, "UEFI", definition.UsesUefi ? "yes" : "no");
        AppendLine(builder, "Estimated transfer", FormatGiB(definition.EstimatedTransferBytes) + " GB");

        AppendSection(builder, "Disks", definition.Disks.Select(BuildDiskLine));
        AppendSection(builder, "Interfaces", definition.Interfaces.Select(BuildInterfaceLine));
        return builder.ToString().Trim();
    }

    internal static string BuildTargetDetails(ProxmoxVirtualMachineDefinition definition)
    {
        if (definition == null)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        AppendLine(builder, "VMID", definition.VmId.ToString(CultureInfo.InvariantCulture));
        AppendLine(builder, "Name", definition.Name);
        AppendLine(builder, "State", definition.Status);
        AppendLine(builder, "Running", definition.Running ? "yes" : "no");
        AppendLine(builder, "Cores", definition.Cores.ToString(CultureInfo.InvariantCulture));
        AppendLine(builder, "Memory", FormatGiBFromMb(definition.MemoryMb) + " GB");

        AppendSection(builder, "Networks", definition.Networks);
        AppendSection(builder, "Raw config", definition.RawConfigLines);
        return builder.ToString().Trim();
    }

    internal static string BuildNetworksSummary(ProxmoxVirtualMachineDefinition definition)
    {
        var items = (definition == null ? null : definition.Networks) ?? new List<string>();
        return items.Count == 0 ? "-" : string.Join(", ", items);
    }

    internal static string FormatGiBFromKiB(long value)
    {
        return (Math.Max(0L, value) / 1024D / 1024D).ToString("0.00", CultureInfo.InvariantCulture);
    }

    internal static string FormatGiBFromMb(long value)
    {
        return (Math.Max(0L, value) / 1024D).ToString("0.00", CultureInfo.InvariantCulture);
    }

    internal static string FormatGiB(long bytes)
    {
        return (Math.Max(0L, bytes) / 1073741824D).ToString("0.00", CultureInfo.InvariantCulture);
    }

    private static string BuildDiskLine(VirtualMachineDiskDefinition disk)
    {
        if (disk == null)
        {
            return null;
        }

        var source = !string.IsNullOrWhiteSpace(disk.SourcePath)
            ? disk.SourcePath
            : (!string.IsNullOrWhiteSpace(disk.SourcePool) && !string.IsNullOrWhiteSpace(disk.SourceVolume)
                ? disk.SourcePool + "/" + disk.SourceVolume
                : "-");
        var sizeBytes = disk.ActualSizeBytes > 0 ? disk.ActualSizeBytes : disk.VirtualSizeBytes;

        return (disk.TargetName ?? "disk") +
               " | " + source +
               " | format=" + (string.IsNullOrWhiteSpace(disk.Format) ? "unknown" : disk.Format) +
               " | size=" + FormatGiB(sizeBytes) + " GB";
    }

    private static string BuildInterfaceLine(VirtualMachineInterfaceDefinition nic)
    {
        if (nic == null)
        {
            return null;
        }

        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(nic.InterfaceName))
        {
            parts.Add(nic.InterfaceName);
        }

        if (!string.IsNullOrWhiteSpace(nic.Type))
        {
            parts.Add("type=" + nic.Type);
        }

        if (!string.IsNullOrWhiteSpace(nic.SourceName))
        {
            parts.Add("source=" + nic.SourceName);
        }

        if (!string.IsNullOrWhiteSpace(nic.Model))
        {
            parts.Add("model=" + nic.Model);
        }

        if (!string.IsNullOrWhiteSpace(nic.MacAddress))
        {
            parts.Add("mac=" + nic.MacAddress);
        }

        return parts.Count == 0 ? null : string.Join(" | ", parts);
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
}
