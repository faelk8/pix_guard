/** Controla o índice lateral da página, preservando a escolha nesta sessão. */
document.addEventListener("DOMContentLoaded", () => {
  const button = document.getElementById("course-secondary-toggle");
  const sidebar = document.getElementById("pst-secondary-sidebar");
  if (!button) return;
  if (!sidebar) {
    button.hidden = true;
    return;
  }

  const key = "pixguard-ai-secondary-sidebar";
  let closed = false;
  try {
    closed = sessionStorage.getItem(key) === "closed";
  } catch (_) {
    // Armazenamento desabilitado: o controle continua funcionando nesta página.
  }

  function render() {
    document.documentElement.dataset.secondarySidebar = closed ? "closed" : "open";
    button.setAttribute("aria-expanded", String(!closed));
    button.textContent = closed ? "Exibir índice da página" : "Ocultar índice da página";
    button.setAttribute("aria-label", button.textContent);
  }

  button.addEventListener("click", () => {
    closed = !closed;
    try {
      sessionStorage.setItem(key, closed ? "closed" : "open");
    } catch (_) {
      // A ausência de armazenamento não impede a alternância manual.
    }
    render();
  });
  render();
});
