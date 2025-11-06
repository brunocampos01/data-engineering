# Windows
On Windows, I recommend using the package manager [chocolatey](https://chocolatey.org/), but first make sure that you can execute the Powershell as admin:

Open terminal as admin:
<img src='../../images/windows_choco_1.png' height=auto width="100%">

<br/>

Get privilages to execute the Powershell as **admin**:
```powershell
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser -Force
```

Install [Chocolatey](https://chocolatey.org/install):
```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1')) 
```

<img src='../../images/windows_choco_2.png' height=auto width="100%">

<br/>

Test:
<img src='../../images/windows_choco_3.png' height=auto width="100%">

<br/>

Execute script to install applications:
```powershell
PowerShell -ExecutionPolicy Bypass .\run.ps1
```

---
