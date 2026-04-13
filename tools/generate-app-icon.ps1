param(
    [string]$PngPath = "D:\codex\assets\DockerSynologyMigrator.png",
    [string]$IcoPath = "D:\codex\assets\DockerSynologyMigrator.ico"
)

$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Drawing

function New-RoundedRectanglePath {
    param(
        [float]$X,
        [float]$Y,
        [float]$Width,
        [float]$Height,
        [float]$Radius
    )

    $path = New-Object System.Drawing.Drawing2D.GraphicsPath
    $diameter = [Math]::Max(2, $Radius * 2)
    $path.AddArc($X, $Y, $diameter, $diameter, 180, 90)
    $path.AddArc($X + $Width - $diameter, $Y, $diameter, $diameter, 270, 90)
    $path.AddArc($X + $Width - $diameter, $Y + $Height - $diameter, $diameter, $diameter, 0, 90)
    $path.AddArc($X, $Y + $Height - $diameter, $diameter, $diameter, 90, 90)
    $path.CloseFigure()
    return $path
}

function Write-IcoFromPngBytes {
    param(
        [byte[]]$PngBytes,
        [string]$Path
    )

    $directory = [System.IO.Path]::GetDirectoryName($Path)
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    }

    $stream = [System.IO.File]::Create($Path)
    try {
        $writer = New-Object System.IO.BinaryWriter($stream)
        $writer.Write([UInt16]0)
        $writer.Write([UInt16]1)
        $writer.Write([UInt16]1)
        $writer.Write([byte]0)
        $writer.Write([byte]0)
        $writer.Write([byte]0)
        $writer.Write([byte]0)
        $writer.Write([UInt16]1)
        $writer.Write([UInt16]32)
        $writer.Write([UInt32]$PngBytes.Length)
        $writer.Write([UInt32]22)
        $writer.Write($PngBytes)
        $writer.Flush()
    }
    finally {
        $stream.Dispose()
    }
}

$size = 256
$bitmap = New-Object System.Drawing.Bitmap($size, $size, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)

try {
    $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
    $graphics.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
    $graphics.Clear([System.Drawing.Color]::Transparent)

    $bgPath = New-RoundedRectanglePath 16 16 224 224 42
    $bgBrush = New-Object System.Drawing.Drawing2D.LinearGradientBrush(
        ([System.Drawing.Point]::new(16, 16)),
        ([System.Drawing.Point]::new(240, 240)),
        ([System.Drawing.Color]::FromArgb(255, 29, 86, 96)),
        ([System.Drawing.Color]::FromArgb(255, 19, 60, 69))
    )
    $graphics.FillPath($bgBrush, $bgPath)

    $shadowBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(40, 0, 0, 0))
    $graphics.FillEllipse($shadowBrush, 38, 150, 170, 36)

    $nasPath = New-RoundedRectanglePath 38 52 104 88 18
    $nasBrush = New-Object System.Drawing.Drawing2D.LinearGradientBrush(
        ([System.Drawing.Point]::new(38, 52)),
        ([System.Drawing.Point]::new(142, 140)),
        ([System.Drawing.Color]::FromArgb(255, 53, 58, 63)),
        ([System.Drawing.Color]::FromArgb(255, 29, 33, 37))
    )
    $graphics.FillPath($nasBrush, $nasPath)
    $graphics.DrawPath((New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(90, 255, 255, 255), 2)), $nasPath)

    $screenBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 23, 26, 29))
    $graphics.FillRectangle($screenBrush, 54, 68, 54, 12)

    $linePen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(95, 255, 255, 255), 2)
    foreach ($x in 58, 76, 94, 112) {
        $graphics.DrawLine($linePen, $x, 88, $x, 126)
    }

    $statusBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 255, 188, 71))
    $graphics.FillEllipse($statusBrush, 118, 67, 9, 9)
    $graphics.FillEllipse((New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 122, 219, 128))), 130, 67, 9, 9)

    $font = New-Object System.Drawing.Font("Segoe UI", 10, [System.Drawing.FontStyle]::Bold, [System.Drawing.GraphicsUnit]::Pixel)
    $graphics.DrawString("S", $font, ([System.Drawing.Brushes]::White), 56, 97)

    $whaleBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 38, 130, 216))
    $whaleDarkBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 27, 98, 165))
    $graphics.FillEllipse($whaleBrush, 110, 132, 92, 44)
    $graphics.FillRectangle($whaleBrush, 118, 118, 76, 24)
    $graphics.FillPie($whaleBrush, 188, 136, 28, 26, 290, 160)

    $tail = New-Object System.Drawing.Point[] 3
    $tail[0] = [System.Drawing.Point]::new(108, 144)
    $tail[1] = [System.Drawing.Point]::new(90, 130)
    $tail[2] = [System.Drawing.Point]::new(95, 156)
    $graphics.FillPolygon($whaleBrush, $tail)

    $fin = New-Object System.Drawing.Point[] 3
    $fin[0] = [System.Drawing.Point]::new(160, 174)
    $fin[1] = [System.Drawing.Point]::new(180, 188)
    $fin[2] = [System.Drawing.Point]::new(147, 187)
    $graphics.FillPolygon($whaleDarkBrush, $fin)

    $graphics.FillEllipse([System.Drawing.Brushes]::White, 178, 146, 7, 7)
    $graphics.DrawArc((New-Object System.Drawing.Pen([System.Drawing.Color]::White, 2)), 166, 150, 20, 12, 15, 120)

    $cubeBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 212, 237, 255))
    $cubeBorder = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(255, 171, 212, 245), 2)
    foreach ($cube in @(
        @{ X = 134; Y = 96 },
        @{ X = 156; Y = 96 },
        @{ X = 178; Y = 96 },
        @{ X = 145; Y = 76 },
        @{ X = 167; Y = 76 }
    )) {
        $graphics.FillRectangle($cubeBrush, $cube.X, $cube.Y, 16, 16)
        $graphics.DrawRectangle($cubeBorder, $cube.X, $cube.Y, 16, 16)
    }

    $arrowPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(210, 255, 255, 255), 6)
    $arrowPen.StartCap = [System.Drawing.Drawing2D.LineCap]::Round
    $arrowPen.EndCap = [System.Drawing.Drawing2D.LineCap]::ArrowAnchor
    $graphics.DrawArc($arrowPen, 74, 118, 92, 58, 210, 120)

    $borderPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(55, 255, 255, 255), 2)
    $graphics.DrawPath($borderPen, $bgPath)

    $pngDirectory = [System.IO.Path]::GetDirectoryName($PngPath)
    if (-not [string]::IsNullOrWhiteSpace($pngDirectory)) {
        [System.IO.Directory]::CreateDirectory($pngDirectory) | Out-Null
    }

    $bitmap.Save($PngPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $pngStream = New-Object System.IO.MemoryStream
    try {
        $bitmap.Save($pngStream, [System.Drawing.Imaging.ImageFormat]::Png)
        Write-IcoFromPngBytes -PngBytes $pngStream.ToArray() -Path $IcoPath
    }
    finally {
        $pngStream.Dispose()
    }
}
finally {
    $graphics.Dispose()
    $bitmap.Dispose()
}

Write-Host "Generated icon:"
Write-Host " PNG: $PngPath"
Write-Host " ICO: $IcoPath"
