/**
 * Dark mode toggle - persists via localStorage.
 */
(function () {
    var STORAGE_KEY = "theme";

    function getPreferred() {
        var stored = localStorage.getItem(STORAGE_KEY);
        if (stored) return stored;
        return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }

    function apply(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        localStorage.setItem(STORAGE_KEY, theme);

        var btn = document.getElementById("theme-toggle");
        if (btn) {
            btn.innerHTML = theme === "dark" ? "&#9788;" : "&#9790;";
            btn.title = theme === "dark" ? "Modo claro" : "Modo escuro";
        }
    }

    apply(getPreferred());

    document.addEventListener("DOMContentLoaded", function () {
        apply(getPreferred());

        var btn = document.getElementById("theme-toggle");
        if (btn) {
            btn.addEventListener("click", function () {
                var current = document.documentElement.getAttribute("data-theme");
                apply(current === "dark" ? "light" : "dark");
            });
        }
    });
})();
