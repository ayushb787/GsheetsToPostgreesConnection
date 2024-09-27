//This is the file which contains code for scripts that you need to add before using this system

//This piece of code will keep on updating last_updated datetime on sheets automatically

//Which is vbery important considering our logic revolves around last_updated datetime

function onEdit(e) {
  let range = e.range;
  let col = range.getColumn();
  let row = range.getRow();
  let sheetName = e.source.getActiveSheet().getName();

  if (sheetName == 'Sheet1' && col != 9 && row > 1) {
    let timestamp = Utilities.formatDate(new Date(),"UTC","yyyy-MM-dd HH:mm:ss");
    let sheet = e.source.getActiveSheet();
    sheet.getRange(row,9).setValue(timestamp);
  }
}
