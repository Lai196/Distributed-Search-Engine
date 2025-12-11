# Remove BOM from all Java files
$javaFiles = Get-ChildItem -Path "src" -Filter "*.java" -Recurse
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
$count = 0

foreach ($file in $javaFiles) {
    try {
        $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
        if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
            $content = [System.IO.File]::ReadAllText($file.FullName, [System.Text.Encoding]::UTF8)
            [System.IO.File]::WriteAllText($file.FullName, $content, $utf8NoBom)
            Write-Host "Removed BOM from: $($file.FullName)"
            $count++
        }
    } catch {
        Write-Host "Error processing $($file.FullName): $_" -ForegroundColor Red
    }
}

if ($count -gt 0) {
    Write-Host "Removed BOM from $count file(s)." -ForegroundColor Green
}

