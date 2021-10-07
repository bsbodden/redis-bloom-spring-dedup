var slider = document.getElementById("range");

var slideCol = document.getElementById("ranges");
var y = document.getElementById("f");
y.innerHTML = slideCol.value;

slideCol.oninput = function () {
  y.innerHTML = this.value;
}

function updateTextInput(val) {
  document.getElementById('range').value = val;
}