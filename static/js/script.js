function resetPage() {
  document.getElementById("search-type").value = "";
  toggleSearchFields();
  document.getElementById("results-title").style.display = "none";
  document.getElementById("loading-image").style.display = "none"; // Oculta a imagem de carregamento ao carregar a página
}

// Mostra o GIF de carregamento ao enviar qualquer formulário de pesquisa
document.addEventListener("submit", function (event) {
  document.getElementById("loading-image").style.display = "flex";
});

// Oculta o GIF de carregamento após o carregamento da página com novos resultados
document.addEventListener("DOMContentLoaded", function () {
  document.getElementById("loading-image").style.display = "none"; // Esconde o GIF ao completar o carregamento inicial
});

// Esconde o GIF após o término da exportação e unificação usando um truque com tempo (melhorar com backend seria ideal)
function hideLoadingImage() {
  document.getElementById("loading-image").style.display = "none";
}

// Eventos para monitorar quando o Excel é exportado ou unificado
document.querySelector("form[action*='export_excel']").onsubmit = function () {
  document.getElementById("loading-image").style.display = "flex";
  setTimeout(hideLoadingImage, 3000); // Ajuste o tempo conforme necessário
};

document.querySelector("form[action*='process_excel']").onsubmit = function () {
  document.getElementById("loading-image").style.display = "flex";
  setTimeout(hideLoadingImage, 3000); // Ajuste o tempo conforme necessário
};

function openTab(evt, tabName) {
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tab-content");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablink");
  for (i = 0; i < tablinks.length; i++) {
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  document.getElementById(tabName).style.display = "block";
  evt.currentTarget.className += " active";
}

function toggleSearchFields() {
  const searchType = document.getElementById("search-type").value;
  document.getElementById("cnpj-form").style.display = "none";
  document.getElementById("nome-cpf-form").style.display = "none";
  document.getElementById("coringa-form").style.display = "none";
  document.getElementById("unificar-excel-form").style.display = "none";

  if (searchType === "cnpj") {
    document.getElementById("cnpj-form").style.display = "block";
  } else if (searchType === "nome-cpf") {
    document.getElementById("nome-cpf-form").style.display = "block";
  } else if (searchType === "coringa") {
    document.getElementById("coringa-form").style.display = "block";
  } else if (searchType === "unificar-excel") {
    document.getElementById("unificar-excel-form").style.display = "block";
  }
}
