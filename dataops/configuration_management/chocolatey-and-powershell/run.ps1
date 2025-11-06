#******************************************************************************
# Logs
#******************************************************************************
$ErrorActionPreference = "Continue"
filter timestamp {"[$(Get-Date -Format G)]: $_"}
Write-Output "Script started with UTC zone." | timestamp

#******************************************************************************
# Modules and policy
#******************************************************************************
# Check if module PowerShellGet is installed
If (Get-Module -ListAvailable -Name PowerShellGet) {
    Write-Output "Module: PowerShellGet exists" | timestamp

} Else {
    # Install PowerShellGet
    Install-Module PowerShellGet -Force -AllowClobber -SkipPublisherCheck | timestamp # -Force -AllowClobber to auto confirm
}

# Check if module posh-git is installed
If (Get-Module -ListAvailable -Name posh-git) {
    Write-Output "Module: posh-git exists" | timestamp

} Else {
    # Install git
    Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Force
    Install-Module posh-git -Scope CurrentUser -Force -AllowClobber # -Force -AllowClobber to auto confirm
    Import-Module posh-git
    'Import-Module Posh-Git' | Out-File -Append -Encoding default -FilePath $profile
    Add-PoshGitToProfile -AllHosts
}

# Check if choco is installed
If (Test-Path -Path "$env:ProgramData\Chocolatey") {
    Write-Output "Chocolatey already Installed" | timestamp

} Else {
    Write-Output "Chocolatey is not Installed. Installing ..." | timestamp
    # Install chocolatey
    iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))
    SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"

    # .NET and git need reboot
    choco upgrade chocolatey | timestamp
    choco install git.install --force --yes --ignore-checksums --no-progress | timestamp
    choco install dotnetfx --force --yes --ignore-checksums --no-progress | timestamp
    Restart-Computer -Force -Wait| timestamp
}

Write-Output "Modules and policy FINISHED"| timestamp

#******************************************************************************
# Download and install application
#******************************************************************************
Write-Output "Chocolatey install application ..." | timestamp

# Install application - choco
Powershell .\choco_install_app.ps1 | timestamp

Write-Output "Download and install application FINISHED. Rebooting in 10 min..."| timestamp
#******************************************************************************
# Reboot
#******************************************************************************
Start-Sleep -s 60
Restart-Computer -Force
