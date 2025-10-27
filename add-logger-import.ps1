# مسیر ریشه‌ی پروژه
$root = "C:\Users\Administrator\Desktop\Projects\rahin"

# اطمینان از وجود logger.js
$loggerPath = Join-Path $root "logger.js"
if (-not (Test-Path $loggerPath)) {
    Write-Host "❌ فایل logger.js در ریشه پیدا نشد. مسیر را بررسی کن:" -ForegroundColor Red
    Write-Host $loggerPath
    exit
}

# تابع محاسبه مسیر نسبی تا logger.js
function Get-RelativeImportPath($fileFullPath, $rootPath) {
    $relativePart = (Split-Path $fileFullPath -Parent).Substring($rootPath.Length)
    $segments = ($relativePart -split '\\') | Where-Object { $_ -ne '' }
    $depth = $segments.Count
    if ($depth -eq 0) {
        return "./logger.js"
    } else {
        $prefix = "../" * $depth
        return "${prefix}logger.js"
    }
}

# اسکن همه‌ی فایل‌های JS (غیر از node_modules)
Get-ChildItem -Path $root -Recurse -Filter *.js |
Where-Object { $_.FullName -notmatch "node_modules" } |
ForEach-Object {
    $file = $_.FullName
    $content = Get-Content -Raw -Encoding UTF8 -Path $file

    if ($content -notmatch 'import\s+["''].*logger\.js["'']') {

        $importPath = Get-RelativeImportPath -fileFullPath $file -rootPath $root
        $importLine = "import '$importPath';`r`n"

        # اضافه کردن به ابتدای فایل
        $newContent = $importLine + $content
        Set-Content -Path $file -Value $newContent -Encoding UTF8

        Write-Host "✅ Added import to: $file"
    } else {
        Write-Host "✔️ Already has import: $file"
    }
}
