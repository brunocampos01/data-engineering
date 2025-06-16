// ###########################################
// ADD ITEMS
// ###########################################
function addItemInventory() {
  const { entradasSheet, saidasSheet, estoqueSheet, userEmail, today } = initializeSheets();
  const data = getData(entradasSheet);

  const isValid = validateData(data, 'entradas');
  if (!isValid) return;

  appendDataToInventory(data, estoqueSheet, userEmail, today);
  consolidateInventory(estoqueSheet);
  logTransaction("Entrada", data, userEmail, today);
  clearSheet(entradasSheet);
  showAlert("Itens adicionados ao estoque!");
}

function initializeSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const entradasSheet = ss.getSheetByName("Entradas");
  const saidasSheet = ss.getSheetByName("Saidas");
  const estoqueSheet = ss.getSheetByName("Estoque");
  const userEmail = Session.getActiveUser().getEmail() || "usuário desconhecido";
  
  const timezoneOffset = -3;
  const rawToday = new Date();
  const adjustedDate = new Date(rawToday.getTime() - 3 * 60 * 60 * 1000).toISOString();
  const today = adjustedDate.replace('T', ' ').substring(0, 19);

  return { entradasSheet, saidasSheet, estoqueSheet, userEmail, today };
}

// Get data from "entradas"
function getData(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 6) return []; // No data from row 6

  const dataRange = sheet.getRange(`A6:D${lastRow}`);
  return dataRange.getValues();
}

// Get data from "estoque"
function getInventoryData(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return []; // with header
  const dataRange = sheet.getRange(`A2:D${lastRow}`);
  return dataRange.getValues();
}

function getSaidasData(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 6) return [];

  const dataRange = sheet.getRange(`A6:E${lastRow}`);
  const data = dataRange.getValues();

  // Filter rows that are NOT completely empty
  const filteredData = data.filter(row => {
    const isRowEmpty = row.every(cell => cell === "" || cell === null);
    return !isRowEmpty;
  });

  return filteredData;
}

// Validate
function validateData(data, transactionType) {
  let hasValidRow = false;

  for (let i = 0; i < data.length; i++) {
    const row = data[i];

    const categoria = row[0];
    const nomeDoItem = row[1];
    const localizacao = row[2];
    const quantidade = row[3];

    const isRowEmpty = row.every(cell => cell === "" || cell === null);
    if (isRowEmpty) continue;
    hasValidRow = true;

    if (!categoria) {
      showAlert(`❌ Erro: "Categoria" está vazia.`);
      return false;
    }
    if (!nomeDoItem) {
      showAlert(`❌ Erro: "Nome do Item" está vazio.`);
      return false;
    }
    if (!localizacao) {
      showAlert(`❌ Erro: "Localização Almox" está vazia.`);
      return false;
    }
    if (quantidade === "" || quantidade === null) {
      showAlert(`❌ Erro: "Quantidade" está vazia`);
      return false;
    }
    if (isNaN(quantidade)) {
      showAlert(`❌ Erro: "Quantidade" não é um número.`);
      return false;
    }
    if (Number(quantidade) <= 0) {
      showAlert(`❌ Erro: "Quantidade" deve ser maior que zero.`);
      return false;
    }

    if (transactionType === 'saidas') {
      const unidadeDestino = row[4];
      if (!unidadeDestino) {
        showAlert(`❌ Erro: "Unidade Destino" está vazia.`);
        return false;
      }
    }
  }

  if (!hasValidRow) {
    showAlert("❌ Não há itens preenchidos.");
    return false;
  }

  return true;
}

function clearSheet(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow >= 6) {
    sheet.getRange(`A6:E${lastRow}`).clearContent();
  }
}

// Append
function appendDataToInventory(data, estoqueSheet, userEmail, today) {
  data.forEach(row => {
    const isRowEmpty = row.every(cell => cell === "" || cell === null);
    if (isRowEmpty) return;

    const completedRow = [row[0], row[1], row[2], row[3], today, userEmail];
    estoqueSheet.appendRow(completedRow);
  });
}

// When "estoque" has 2 or more equal [categoria, nomeDoItem, localizacao], than consolidadte to return only 1 row
function consolidateInventory(estoqueSheet) {
  const lastRow = estoqueSheet.getLastRow();
  if (lastRow < 2) return; // No data

  const dataRange = estoqueSheet.getRange(2, 1, lastRow - 1, 6); // From row 2 (A:F)
  const data = dataRange.getValues();

  const consolidatedMap = {};

  data.forEach(row => {
    const categoria = row[0];
    const nomeDoItem = row[1];
    const localizacao = row[2];
    const quantidade = Number(row[3]) || 0;
    const dataHora = row[4];
    const usuario = row[5];

    const key = `${categoria}-${nomeDoItem}-${localizacao}`.toLowerCase();

    if (consolidatedMap[key]) {
      consolidatedMap[key].quantidade += quantidade;
    } else {
      consolidatedMap[key] = {
        categoria,
        nomeDoItem,
        localizacao,
        quantidade,
        dataHora,
        usuario
      };
    }
  });

  // clear
  estoqueSheet.getRange(2, 1, lastRow - 1, 6).clearContent();

  // filter only items with quantidade > 0
  const consolidatedData = Object.values(consolidatedMap)
    // .filter(item => item.quantidade > 0) // clean the "estoque" removing items with 0
    .map(item => [
      item.categoria,
      item.nomeDoItem,
      item.localizacao,
      item.quantidade,
      item.dataHora,
      item.usuario
    ]);


  // Write back
  estoqueSheet.getRange(2, 1, consolidatedData.length, 6).setValues(consolidatedData);
}

function showAlert(message) {
  SpreadsheetApp.getUi().alert(message);
}

// ###########################################
// TRANSACTIONS LOGS
// ###########################################
function logTransaction(acao, data, userEmail, today) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const transacoesSheet = ss.getSheetByName("transacoes");
  //showAlert("trasacao");

  data.forEach(row => {
    const isRowEmpty = row.every(cell => cell === "" || cell === null);
    if (isRowEmpty) return;

    const categoria = row[0];
    const nomeDoItem = row[1];
    const localizacao = row[2] || "";
    const quantidade = Number(row[3]);
    const unidadeDestino = row[4] || "";

    const logRow = [today, acao, categoria, nomeDoItem, localizacao, quantidade, unidadeDestino, userEmail];
    transacoesSheet.appendRow(logRow);
  });
}

// ###########################################
// DROP ITEMS
// ###########################################
function dropItemInventory() {
  const { entradasSheet, saidasSheet, estoqueSheet, userEmail, today } = initializeSheets();
  const saidasData = getSaidasData(saidasSheet);

  const isValid = validateData(saidasData, 'saidas');
  if (!isValid) return;

  const estoqueData = getInventoryData(estoqueSheet);
  const updatedEstoque = dropDataFromInventory(saidasData, estoqueData, estoqueSheet);

  if (updatedEstoque) {
    logTransaction("Saída", saidasData, userEmail, today);
    clearSheet(saidasSheet);
    showAlert("Itens removidos do estoque!");
  }
}

function dropDataFromInventory(saidaData, estoqueData, estoqueSheet) {
  const estoqueMap = {};
  estoqueData.forEach((row, index) => {
    const key = `${row[0]}-${row[1]}-${row[2]}`.toLowerCase();
    estoqueMap[key] = { index, quantidade: Number(row[3]) };
  });

  // Group saidaData by [categoria, nomeDoItem, localizacao] and sum quantidade
  const saidaGrouped = {};
  for (let i = 0; i < saidaData.length; i++) {
    const row = saidaData[i];
    const categoria = row[0];
    const nomeDoItem = row[1];
    const localizacao = row[2];
    const quantidade = Number(row[3]);

    // Skip invalid/empty rows
    if (!categoria || !nomeDoItem || !localizacao || isNaN(quantidade)) continue;

    const key = `${categoria}-${nomeDoItem}-${localizacao}`.toLowerCase();
    if (saidaGrouped[key]) {
      saidaGrouped[key].quantidade += quantidade;
    } else {
      saidaGrouped[key] = {
        categoria,
        nomeDoItem,
        localizacao,
        quantidade
      };
    }
  }

  // Validate and update estoqueData
  for (const key in saidaGrouped) {
    const { categoria, nomeDoItem, localizacao, quantidade } = saidaGrouped[key];

    if (!(key in estoqueMap)) {
      showAlert(`❌ Erro:\n Item "${nomeDoItem}" na categoria "${categoria}" e na Localização Almox ${localizacao} não existe no estoque.`);
      return false;
    }

    const estoqueItem = estoqueMap[key];
    const quantidadeEstoque = estoqueItem.quantidade;

    if (quantidadeEstoque < quantidade) {
      showAlert(`❌ Erro:\n Quantidade insuficiente para "${nomeDoItem}" na categoria "${categoria}". \n\nQuantidade requerida: ${quantidade}\nQuantidade no estoque: ${quantidadeEstoque}`);
      return false;
    }

    // Update quantity
    estoqueData[estoqueItem.index][3] = quantidadeEstoque - quantidade;
  }

  // Write updated estoqueData
  const range = estoqueSheet.getRange(2, 1, estoqueData.length, estoqueData[0].length);
  range.setValues(estoqueData);

  return true;
}
