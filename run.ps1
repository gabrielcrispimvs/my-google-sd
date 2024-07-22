# Define os diretórios dos scripts
$registryScript = 
$monitorScript = 
$lbInsertScript = 
$insertScript = 
$serverScript = 

# Função para iniciar um script Python e retornar o processo
function Start-PythonScript {
    param (
        [string]$script_args
    )
    return Start-Process -FilePath "python" -ArgumentList $script_args -PassThru
}

# Lista para armazenar os processos iniciados
$processList = @()

# Inicia uma instância de cada serviço e armazena o processo na lista
Write-Output "Iniciando Registry..."
$processList += Start-PythonScript -script_args "registry/registry.py"
Start-Sleep -Seconds 1

Write-Output "Iniciando Monitor..."
$processList += Start-PythonScript -script_args "monitor/monitor.py"
Start-Sleep -Seconds 1

Write-Output "Iniciando Load Balancer..."
$processList += Start-PythonScript -script_args "insert/lb_insert.py"
Start-Sleep -Seconds 1

Write-Output "Iniciando Insert..."
$processList += Start-PythonScript -script_args "insert/insert.py"
Start-Sleep -Seconds 1

# Inicia 10 instâncias do servidor e armazena os processos na lista
for ($i = 1; $i -le 10; $i++) {
    $nodeName = "server_$i"
    Write-Output "Iniciando Server $i com node_name $nodeName..."
    $processList += Start-PythonScript -script_args "server/server.py $nodeName"
}

Write-Output "Todos os serviços foram iniciados. Pressione Enter para fechar todos os serviços."

# Espera pela entrada do usuário
$input = Read-Host

# Mata todos os processos iniciados
foreach ($process in $processList) {
    Write-Output "Fechando processo ID $($process.Id)..."
    Stop-Process -Id $process.Id -Force
}

Write-Output "Todos os serviços foram fechados."
