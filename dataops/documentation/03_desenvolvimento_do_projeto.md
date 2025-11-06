# Requisitos do Projeto

Esta seção contém todos os hardwares, softwares e configurações utilizadas além de um pequeno tutorial de como instalar e configurar cada requisito.

## Requisitos de Hardware

| Parâmetros      | Valores                             |
|-----------------|-------------------------------------|
| Processador     | 2.4 Ghz, 8 processadores            |
| Memória RAM     | 16 GB                               |
| Espaço em disco | 100 GB                              |

## Requisitos de Sistema

| Sistema Operacional                    | Versão            | Arquitetura       |
|----------------------------------------|-------------------|-------------------|
| Windows Server                         | 2016              | 64 bits           |


## Requisitos de Softwares

| Parâmetros                                             | Valores                                    |
|--------------------------------------------------------|--------------------------------------------|
| PowerShell                                             | 5.1.18362.145                              |
| Remote Desktop Web Connection                          | 4.7.2                                      |
| Winrar                                                 | 5.8                                        |
| .NET Framework                                         | 4.8                                        |
| DBeaver                                                | 7.0.1                                      |
| VS code                                                | 1.41.1                                     |
| Python                                                 | 3.8                                        |
| Java                                                   | 1.8                                        |
| Power BI Desktop                                       | 2.79.5768.562                              |
| On-primises Data Gateway                               | 3007.149                                   |
| SQL Server Management Studio (SSMS)                    | 15.0.18206.0                               |
| SQL Server Data Tools for Visual Studio 2017 (SSDT)    | 15.9.18                                    |
| SQL Server 2019 Enterprise Edition (64-bit)            | 15.0.2000.5                                |
| SQL Server Integration Services (SSIS)                 | 15.0                                       |
| SQL Server Analysis Services (SSAS)                    | 15.0                                       |
| SQL Server Agent                                       | 15.0                                       |
| OLE DB Driver for SQL Server                           | 18.3.0                                     |
| ADO .NET Driver for SQL Server                         | 4.8                                        |
| ODBC Driver for SQL Server                             | 17.5.2                                     |
| JDBC Driver for SQL Server                             | 8.2                                        |
| JDBC Driver for BD2                                    | 4                                          |
| módulo: SqlServer                                      | 21.1.18221                                 |

Install-Module -Name SqlServer
---

## Instalação de Requisitos de Forma Automatizada
Para abstrair a complexidade de instalação e configurações foi desenvolvido scripts que automatizam estas etapas. Os softwares, bibliotecas, configurações e o provisionamento de recursos se tornam transparentes para o usuário que for utilizar este projeto.
<br >
Os seguintes processos de instalação e configurações são realizados:

* Faz o download do projeto
* Instala o PowerShellGet (para instalar módulos PowerShell)
* Instala o Git
* Instala o Chocolatey
* Instala o .NET Framework
* Instala o Python
* Instala o Winrar
* Instala o VS Code
* Instala o SQL Server Management Studio (SSMS) 
* Instala o SQL Server Data Tools for Visual Studio 2017 (SSDT)
* Instala o Power BI Desktop
* Instala o Google Chrome
* Faz o download e abre o On-premises Data Gateway

Siga o passo-a-passo abaixo para executar corretamente a automação:

1.	Execute o PowerShell como admin e abra o diretório do usuário: 

![admin_ps1](img/admin_ps1.png)


2.	Cole o comando abaixo no PowerShell para conceder permissão de execução de scripts.
```powershell
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser -Force
```

3.	No PowerShell, digite:
`PowerShell -ExecutionPolicy Bypass .\prepare_env.ps1 -client_name <NOME_CLIENTE> -project_name <NOME_PROJETO> -data_source <TIPO_DATA_SOURCE>`

Exemplo:
```powershell
PowerShell -ExecutionPolicy Bypass .\prepare_env.ps1 `
    -client_name mpce `
    -project_name sajinsights-mpce `
    -data_source postgresql
```

A partir de agora a instalação automatizada vai começar. Toda a saída pode ser acompanhada no arquivo de log `log_installation.txt` e no PowerShell.

<br/>
<br/>

## Instalação de Requisitos de Forma Manual

### **.NET Framework** 

As bibliotecas do .NET são necessárias para rodar várias aplicações utilizadas neste projeto.

##### Requisito
É necessário baixar o executável:

 * [.NET Framework 4.8](https://dotnet.microsoft.com/download/dotnet-framework/net48)
  
##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial](https://docs.microsoft.com/pt-br/dotnet/framework/install/on-windows-10)

<br/>
<br/>

### **Drivers para SQL Server**
Os drivers servem para conectar no banco de dados do SQL Server onde é fornecido uma API de comunicação. Os seguintes drivers serão necessários:

* OLE DB
* ADO .NET 
* ODBC
* JDBC
  
##### Requisitos
É necessário baixar os executáveis:

* [OLE DB 18.3.0](https://go.microsoft.com/fwlink/?linkid=2117515)
* [ADO . NET](https://docs.microsoft.com/en-us/sql/connect/ado-net/microsoft-ado-net-sql-server?view=sql-server-ver15)
* [ODBC 17.5.2](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15)
* [JDBC Driver 8.2](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15)

##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja as documentações [oficiais.](https://docs.microsoft.com/en-us/sql/connect/homepage-sql-connection-programming?view=sql-server-ver15)

??? note "Notas"
    * OLE DB é utilizado pelo SSAS e SSIS através do SSDT para acessar o SQL Server.
    * ADO .NET é utilizado pelo SSIS através do SSDT para acessar o banco de dados do DB2.
    * ODBC é utilizado pelo Python para se comunicar com o SQL Server
    * JDBC é utilizado pelo DBeaver para se comunicar com o SQL Server.

<br/>
<br/>

### **Drivers para DB2**
Os drivers servem para conectar no banco de dados DB2 IBM onde é fornecido uma API de comunicação.

##### Requisito
É necessário baixar um arquivo `jar` e um executável pelo site oficial da IBM. Para isto, será necessário criar/logar uma conta de usuário da IBM.

* [JDBC Tipo 4](https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads)
* [IBM Data Server Driver 11.5](https://www.ibm.com/support/fixcentral/swg/selectFixes?source=dbluesearch&product=ibm%2FInformation+Management%2FIBM+Data+Server+Client+Packages&release=11.1.*&searchtype=fix&fixids=*-dsdriver-*FP003&function=fixId&parent=ibm/Information%20Management)

##### Instalação
O `jar` deve ficar no diretório `C:` para que todos os usuários tenham acesso. Recomendo criar um diretório dentro do `C:`.
Para mais informações sobre instalações dos drivers da IBM, acesse este [link](https://www.ibm.com/support/home/product/D016441W66756F92/IBM%20Data%20Server%20Client%20Packages)

<br/>
<br/>

### **SQL Server Data Tools (SSDT)**
<!-- ![run one step](img/ssdt_icon.png) -->

É uma ferramenta de desenvolvimento para criar bancos de dados relacionais do SQL Server, e modelos de dados do AS (Analysis Services).

##### Requisito
É necessário ter baixado o seguinte programa:

* [SSDT (SQL Server Data Studio) 15.9.2](https://go.microsoft.com/fwlink/?linkid=2110080)

##### Instalação

1.	Abra o instalador e clique em **next**. 
2.	Selecione somente 

* [x] SQL Server
* [x] SQL Server Analysis Services
* [ ] SQL Server Reporting Services
* [x] SQL Server Integration Services

<!-- ![ssdt](img/ssdt.png) -->

3. Clique em **install** e aguarde a finalização.

??? note "Notas"
    * Acesse a documentação completa da ferramenta clicando [aqui.](https://docs.microsoft.com/pt-br/sql/ssdt/sql-server-data-tools?view=sql-server-ver15)

<br/>
<br/>

<!-- ![run one step](img/ssms.png) -->
### **SQL Server Management Studio (SSMS)**

É um ambiente de gerenciamento para manipular o Azure Analysis Services.

##### Requisito
É necessário baixar o executável:

* [SQL Server Management Studio 18.3.1](https://go.microsoft.com/fwlink/?linkid=2105412)

##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial](https://docs.microsoft.com/pt-br/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver15#info_tipmediainfo-tippng-get-help-for-sql-tools)

??? note "Notas"
    * Acesse a documentação completa da ferramenta clicando [aqui.](https://docs.microsoft.com/pt-br/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver15)


<br/>

<!-- ![run one step](img/data_gw.png)  -->
### **On-premises Data Gateways** 

O On-premises Data Gateway é necessário para trafegar os dados do data source para o Azure Analysis Services.

##### Requisito
É necessário baixar o executável:

* [On-premises Data Gateways](https://docs.microsoft.com/pt-br/data-integration/gateway/service-gateway-install)

##### Instalação

1. Clique no executável que foi baixado. Em seguida, selecione **Next**.
   
![run one step](img/gateway_step_1.png) 
<br/>

2.	Selecione Gateway de dados local (recomendado), que é o modo padrão e, em seguida, selecione **Next**.

![run one step](img/gateway_step_2.png) 

<br/>

3.	Leia os termos de uso e privacidade e aceite os termos de uso. Em seguida ,selecione **Install**.

![run one step](img/gateway_step_3.png) 

<br/>

4.	Depois que o gateway for instalado com êxito, forneça o endereço de email, em seguida, selecione entrar:

![run one step](img/gateway_step_4.png) 

<br/>

5.	Selecione registrar um novo gateway neste computador e clique em **Next**. Essa etapa registra a instalação do gateway com o serviço de nuvem do gateway.

![run one step](img/gateway_step_5.png) 

<br/>

6.	Forneça estas informações para a configuração do data gateway:

   * Sugestão: `<nome_produto>-<nome_cliente>-onpremisesgw`
   * A chave de recuperação, que deve ter pelo menos oito caracteres.
   * Confirmação para a chave de recuperação

![run one step](img/gateway_step_6.png) 

<br/>

7. Examine as informações na janela de confirmação final. Este exemplo usa a mesma conta para aplicativos lógicos, Power BI, PowerApps e Microsoft Flow, portanto, o gateway está disponível para todos esses serviços. Quando estiver pronto, selecione fechar.

![run one step](img/gateway_step_8.png) 

<br/>

??? note "Notas"
    * Acesse a documentação completa da ferramenta clicando [aqui.](https://docs.microsoft.com/pt-br/power-bi/service-gateway-onprem)
    * O serviço de gateway roda em segundo plano como um serviço do Windows. Para verificar se esta funcionando acesse `run > services` e procure por `On-premises Data Gateway`

<br/>

<!-- ![run one step](img/sql_server.png) -->
### **SQL Server 2019 Enterprise Edition**
Este é o banco de dados onde ficará o a área de stage, o cubo OLAP e o data warehouse.

##### Requisito
É necessário adquirir a licença enterprise. Para o ambiente de teste é possível utilizar o SQL Server Developer que pode ser baixado no seguinte link:

* [SQL Server 2019 Developer Edition](https://www.microsoft.com/pt-br/sql-server/sql-server-downloads)

##### Instalação
* TODO
https://docs.microsoft.com/pt-br/sql/database-engine/install-windows/install-sql-server?view=sql-server-ver15

??? note "Notas"
    * 


#### SQL Server Integration Services
#### SQL Server Analysis Services
#### SQL Server Agent

<br/>
<br/>

<!-- ![run one step](img/python.png)
### Python -->
<!-- ![run one step](img/python-logo-master-v3-TM.png) -->

### Módulo Powershell

```powershell
Install-Module -Name SqlServer
```


### **Python**

##### Requisito
É necessário baixar o executável:

* [Python 3.8.2](https://www.python.org/ftp/python/3.8.2/python-3.8.2.exe)

##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial](https://www.python.org/)

##### Bibliotecas
É necessário instalar as seguites bibliotecas:

* numpy
* pandas
* sklearn
* statsmodel
* matplotlib
* pyodbc
* jupyter
* jupter-lab

Para baixar e instalar abra o powershell e cole o comando abaixo:
```powershell
pip install --user --upgrade numpy pandas sklearn statsmodel matplotlib pyodbc jupyter jupyterlab
```

##### Notas

??? note "Notas"
    * Acesse a documentação completa da liguagem clicando [aqui.](https://dbeaver.io/)

<br/>
<br/>

<!-- ![run one step](img/vscode.png) -->
### **VS Code**

É um editor de código que vem com realce de sintaxe, complementação inteligente de código, snippets e refatoração de código.

##### Requisito
É necessário baixar o executável:

* [VS Code 1.43](https://code.visualstudio.com/)
  
##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação[oficial.](https://code.visualstudio.com/docs)

??? note "Notas"
 Recomendo instalar algumas extensões no editor, como:
  * [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
  * [vccode-icons](https://marketplace.visualstudio.com/items?itemName=vscode-icons-team.vscode-icons)
  * [SQL Server (mssql)](https://marketplace.visualstudio.com/items?itemName=ms-mssql.mssql)


<br/>
<br/>

<!-- ![run one step](img/beaver-head.png) -->
### **DBeaver**
O DBeaver é a aplicação utilizada para acessar as base de dados. 

##### Requisito
É necessário baixar o executável:

* [DBeaver  7.0.1](https://dbeaver.io/download/)

##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial](https://dbeaver.io/)

??? note "Notas"
    * Acesse a documentação completa da ferramenta clicando [aqui.](https://dbeaver.io/)

<br/>
<br/>

<!-- ![run one step](img/java.png) -->
### **Java JDK**
O JDK é necessário para executar o DBeaver e o driver JDBC. Por padrão, o OpenJDK 11 já é instalado junto com o DBeaver.

##### Requisito
É necessário baixar o binário do Java:

* [OpenJDK  11](https://openjdk.java.net/)

##### Instalação
Recomendo seguir [estas](https://stackoverflow.com/questions/52511778/how-to-install-openjdk-11-on-windows) orientações para instalar o openJDK.

??? note "Notas"
    * Para verificar se a instalação ocorreu tudo certo do java, abra o powershell e digite:
    ```bash
    java -version
    ```

<br/>
<br/>

<!-- ![run one step](img/git.png) -->
### **Git**

O git é a ferramenta de controle de versão utilizada neste projeto. 

##### Requisito
É necessário baixar o executável:

* [Git 2.26.0](https://github.com/git-for-windows/git/releases/download/v2.26.0.windows.1/Git-2.26.0-64-bit.exe)
  
##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial](https://git-scm.com/doc)

??? note "Notas"
    * Esta instalação cria um programa chamado git bash, por linha de comando. Caso queria algo mais visual, recomendo o gitkraken ou gitcola.

<br/>
<br/>

<!-- ![run one step](img/winrar.jpg) -->
### **Winrar**

##### Requisito
É necessário baixar o executável:

* [WinRAR 5.80](https://www.win-rar.com/predownload.html?&L=0)
  
##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir.

<br/>
<br/>

<!-- ![run one step](img/powerbi.png) -->
### **Power BI**

É uma solução de análise de negócios que permite visualizar dados, gerar insights, além de gerar dashboards interativos.

##### Requisito
É necessário baixar o executável:

* [Power BI Desktop](https://aka.ms/pbidesktopstore)
  
##### Instalação
A instalação é bem simples, basta abrir o executável e prosseguir. Em caso de dúvidas, veja a documentação [oficial.](https://powerbi.microsoft.com/pt-br/downloads/)

<br >

---
