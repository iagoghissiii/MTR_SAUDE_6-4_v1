/**
 * Logica do dashboard principal (TCC v1.0).
 */

let paginaAtual = 1;
const LIMITE = 20;

document.addEventListener("DOMContentLoaded", () => {
    carregarResumo();
    carregarContratos();

    document.getElementById("btn-filtrar").addEventListener("click", () => {
        paginaAtual = 1;
        carregarContratos();
    });

    document.querySelectorAll("#filtro-valor-min, #filtro-valor-max").forEach(el => {
        el.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                paginaAtual = 1;
                carregarContratos();
            }
        });
    });
});


function getFiltros() {
    return {
        valorMin: document.getElementById("filtro-valor-min").value,
        valorMax: document.getElementById("filtro-valor-max").value,
        ordem: document.getElementById("filtro-ordem").value,
        pagina: paginaAtual,
        limite: LIMITE,
    };
}


function formatarValor(v) {
    if (!v && v !== 0) return "-";
    return v.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}


function formatarData(d) {
    if (!d) return "-";
    return new Date(d).toLocaleDateString("pt-BR");
}


async function carregarResumo() {
    try {
        const data = await fetchDashboardResumo();
        document.getElementById("total-contratos").textContent = data.total_contratos.toLocaleString("pt-BR");
        document.getElementById("score-medio").textContent = "0.00";
    } catch (error) {
        console.error("Erro ao carregar resumo:", error);
        document.getElementById("total-contratos").textContent = "0";
        document.getElementById("score-medio").textContent = "0.00";
    }
}


async function carregarContratos() {
    const loading = document.getElementById("loading");
    const tbody = document.getElementById("contratos-body");

    loading.classList.add("visible");
    tbody.innerHTML = "";

    try {
        const data = await fetchContratos(getFiltros());
        renderizarTabela(data.contratos);
        renderizarPaginacao(data.pagina, data.total_paginas, data.total);
    } catch (error) {
        console.error("Erro ao carregar contratos:", error);
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="empty-state">
                    <p>Erro ao carregar contratos. Verifique se a API esta rodando.</p>
                </td>
            </tr>`;
    } finally {
        loading.classList.remove("visible");
    }
}


function renderizarTabela(contratos) {
    const tbody = document.getElementById("contratos-body");

    if (!contratos || contratos.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="empty-state">
                    <p>Nenhum contrato encontrado</p>
                </td>
            </tr>`;
        return;
    }

    contratos.forEach((c) => {
        const tr = document.createElement("tr");

        const orgaoNome = c.orgao ? (c.orgao.sigla || c.orgao.nome) : "-";
        const fornecedorNome = c.fornecedor ? c.fornecedor.nome : "-";
        const objeto = c.objeto ? (c.objeto.length > 50 ? c.objeto.substring(0, 50) + "..." : c.objeto) : "-";
        const valor = formatarValor(c.valor);

        tr.innerHTML = `
            <td>${c.id}</td>
            <td title="${orgaoNome}">${orgaoNome}</td>
            <td title="${fornecedorNome}">${fornecedorNome.length > 30 ? fornecedorNome.substring(0, 30) + "..." : fornecedorNome}</td>
            <td title="${c.objeto || ""}">${objeto}</td>
            <td>R$ ${valor}</td>
            <td>${formatarData(c.data_inicio)}</td>
            <td>${formatarData(c.data_fim)}</td>
            <td>
                <button class="btn btn-primary btn-small" onclick="verDetalhe(${c.id})">Ver</button>
            </td>
        `;
        tbody.appendChild(tr);
    });
}


function renderizarPaginacao(paginaAtualResp, totalPaginas, total) {
    const container = document.getElementById("paginacao");
    container.innerHTML = "";

    if (totalPaginas <= 1) return;

    const btnAnterior = document.createElement("button");
    btnAnterior.textContent = "Anterior";
    btnAnterior.disabled = paginaAtualResp <= 1;
    btnAnterior.addEventListener("click", () => {
        paginaAtual = paginaAtualResp - 1;
        carregarContratos();
    });
    container.appendChild(btnAnterior);

    const inicio = Math.max(1, paginaAtualResp - 2);
    const fim = Math.min(totalPaginas, paginaAtualResp + 2);

    for (let i = inicio; i <= fim; i++) {
        const btn = document.createElement("button");
        btn.textContent = i;
        if (i === paginaAtualResp) btn.classList.add("active");
        btn.addEventListener("click", () => {
            paginaAtual = i;
            carregarContratos();
        });
        container.appendChild(btn);
    }

    if (fim < totalPaginas) {
        if (fim < totalPaginas - 1) {
            const dots = document.createElement("span");
            dots.textContent = "...";
            dots.className = "pag-dots";
            container.appendChild(dots);
        }
        const btnUltima = document.createElement("button");
        btnUltima.textContent = totalPaginas;
        btnUltima.addEventListener("click", () => {
            paginaAtual = totalPaginas;
            carregarContratos();
        });
        container.appendChild(btnUltima);
    }

    const btnProximo = document.createElement("button");
    btnProximo.textContent = "Proximo";
    btnProximo.disabled = paginaAtualResp >= totalPaginas;
    btnProximo.addEventListener("click", () => {
        paginaAtual = paginaAtualResp + 1;
        carregarContratos();
    });
    container.appendChild(btnProximo);

    const info = document.createElement("span");
    info.style.marginLeft = "0.75rem";
    info.style.color = "var(--text-muted)";
    info.style.fontSize = "0.8rem";
    info.textContent = `${total} contratos`;
    container.appendChild(info);
}


function verDetalhe(id) {
    window.location.href = `pages/contratos.html?id=${id}`;
}
