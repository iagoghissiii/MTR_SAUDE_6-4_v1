/**
 * Logica da pagina de detalhe do contrato (TCC v1.0).
 */

document.addEventListener("DOMContentLoaded", function () {
    var params = new URLSearchParams(window.location.search);
    var contratoId = params.get("id");

    if (!contratoId) {
        document.getElementById("loading").classList.remove("visible");
        document.getElementById("sem-contrato").style.display = "block";
        return;
    }

    carregarContrato(contratoId);
});


async function carregarContrato(id) {
    try {
        var c = await fetchContratoDetalhe(id);
        preencherDados(c);
    } catch (error) {
        console.error("Erro ao carregar contrato:", error);
        document.getElementById("loading").classList.remove("visible");
        document.getElementById("sem-contrato").style.display = "block";
    }
}


function formatarValor(v) {
    if (!v && v !== 0) return "-";
    return "R$ " + v.toLocaleString("pt-BR", { minimumFractionDigits: 2 });
}


function formatarData(d) {
    if (!d) return "?";
    return new Date(d).toLocaleDateString("pt-BR");
}


function calcularDuracao(inicio, fim) {
    if (!inicio || !fim) return "-";
    var d1 = new Date(inicio);
    var d2 = new Date(fim);
    var dias = Math.round((d2 - d1) / (1000 * 60 * 60 * 24));
    if (dias < 30) return dias + " dias";
    var meses = Math.round(dias / 30.44);
    if (meses < 12) return meses + " meses";
    var anos = Math.floor(meses / 12);
    var resto = meses % 12;
    return anos + " ano" + (anos > 1 ? "s" : "") + (resto > 0 ? " e " + resto + " meses" : "");
}


function preencherDados(c) {
    document.getElementById("loading").classList.remove("visible");
    document.getElementById("conteudo").style.display = "block";

    // Contrato
    document.getElementById("contrato-numero").textContent = "#" + (c.numero || c.id);
    document.getElementById("det-objeto").textContent = c.objeto || "Nao informado";
    document.getElementById("det-orgao").textContent = c.orgao ? c.orgao.nome : "Nao informado";
    document.getElementById("det-fornecedor").textContent = c.fornecedor ? c.fornecedor.nome : "Nao informado";
    document.getElementById("det-cpf-cnpj").textContent = c.fornecedor ? c.fornecedor.cpf_cnpj : "Nao informado";
    document.getElementById("det-valor").textContent = formatarValor(c.valor);
    document.getElementById("det-modalidade").textContent = c.modalidade_licitacao || "Nao informado";
    document.getElementById("det-processo").textContent = c.processo_licitatorio || "Nao informado";

    var inicio = formatarData(c.data_inicio);
    var fim = formatarData(c.data_fim);
    document.getElementById("det-periodo").textContent = inicio + " a " + fim;

    // Score de Anomalia
    document.getElementById("score-anomalia").textContent = "0.0000";

    let riscoNivel = "BAIXO";
    let riscoCor = "#28a745";
    let riscoBg = "rgba(40, 167, 69, 0.2)";
    let barraWidth = "25%";

    document.getElementById("nivel-risco").textContent = riscoNivel;
    document.getElementById("nivel-risco").style.color = riscoCor;
    document.getElementById("nivel-risco").style.backgroundColor = riscoBg;
    document.getElementById("barra-risco").style.backgroundColor = riscoCor;
    document.getElementById("barra-risco").style.width = barraWidth;

    // Sinalizacoes
    var listaAnomalias = document.getElementById("lista-anomalias");
    listaAnomalias.innerHTML = "";

    if (c.anomalias && c.anomalias.length > 0) {
        c.anomalias.forEach(function (an) {
            let ehGrave = false;
            if (an.tipo && (an.tipo.toLowerCase().includes("outlier") || an.tipo.toLowerCase().includes("elevada") || an.tipo.toLowerCase().includes("critico"))) {
                ehGrave = true;
            }

            var bgColor = ehGrave ? "rgba(255, 193, 7, 0.05)" : "rgba(0, 123, 255, 0.05)";
            var borderColor = ehGrave ? "rgba(255, 193, 7, 0.2)" : "rgba(0, 123, 255, 0.2)";
            var iconColor = ehGrave ? "#ffc107" : "#007bff";

            var div = document.createElement("div");
            div.style.backgroundColor = bgColor;
            div.style.border = "1px solid " + borderColor;
            div.style.padding = "1rem";
            div.style.borderRadius = "8px";
            div.style.display = "flex";
            div.style.alignItems = "flex-start";
            div.style.gap = "0.75rem";

            var iconContainer = document.createElement("div");
            iconContainer.style.marginTop = "0.2rem";

            if (ehGrave) {
                iconContainer.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="${iconColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>`;
            } else {
                var bolinha = document.createElement("div");
                bolinha.style.width = "8px";
                bolinha.style.height = "8px";
                bolinha.style.borderRadius = "50%";
                bolinha.style.backgroundColor = iconColor;
                bolinha.style.marginTop = "0.25rem";
                iconContainer.appendChild(bolinha);
            }

            var textContainer = document.createElement("div");
            var title = document.createElement("div");
            title.textContent = an.tipo || "Anomalia Detectada";
            title.style.fontWeight = "600";
            title.style.fontSize = "0.95rem";
            title.style.marginBottom = "0.2rem";

            var desc = document.createElement("div");
            desc.textContent = an.descricao || "Este indicador contribuiu para a pontuacao de anomalia do contrato.";
            desc.style.fontSize = "0.85rem";
            desc.style.color = "var(--text-muted)";
            desc.style.lineHeight = "1.4";

            textContainer.appendChild(title);
            textContainer.appendChild(desc);

            div.appendChild(iconContainer);
            div.appendChild(textContainer);
            listaAnomalias.appendChild(div);
        });
    } else {
        var empty = document.createElement("div");
        empty.textContent = "Nenhuma anomalia sinalizada ou cálculo pendente.";
        empty.style.color = "var(--text-muted)";
        empty.style.fontSize = "0.9rem";
        empty.style.padding = "0.5rem 0";
        listaAnomalias.appendChild(empty);
    }
}

async function consultarReceitaFederal() {
    var cnpjText = document.getElementById("det-cpf-cnpj").innerText;
    var cnpj = cnpjText.replace(/[^0-9]/g, '');

    if (!cnpj || cnpj.length !== 14) {
        alert("CNPJ inválido ou não disponível para consulta.");
        return;
    }

    var btn = document.getElementById("btn-receita");
    var resultDiv = document.getElementById("receita-resultado");

    btn.textContent = "Consultando...";
    btn.disabled = true;

    try {
        var response = await fetch(`https://brasilapi.com.br/api/cnpj/v1/${cnpj}`);
        if (!response.ok) {
            throw new Error("Erro na consulta. CNPJ não encontrado ou limite de requisições excedido.");
        }
        var data = await response.json();

        var situacao = data.descricao_situacao_cadastral || "Desconhecida";
        var isSuspicious = situacao !== "ATIVA";
        var statusColor = isSuspicious ? "#dc3545" : "#28a745";

        var dtAbertura = data.data_inicio_atividade || "Desconhecida";
        let dtAberturaFormatada = dtAbertura;
        if (dtAbertura !== "Desconhecida" && dtAbertura.includes("-")) {
            const partes = dtAbertura.split("-");
            if (partes.length === 3) {
                dtAberturaFormatada = `${partes[2]}/${partes[1]}/${partes[0]}`;
            }
        }

        var capitalSocial = data.capital_social || 0;

        let alertasHTML = "";

        if (isSuspicious) {
            alertasHTML += `<div style="color: #dc3545; font-weight: 500; margin-top: 0.5rem; padding: 0.75rem; background: rgba(220, 53, 69, 0.1); border-radius: 4px; border-left: 4px solid #dc3545;">⚠️ Atenção: A situação cadastral da empresa não é ATIVA. (Status Atual: ${situacao})</div>`;
        }

        if (dtAbertura !== "Desconhecida") {
            var dataAberturaDate = new Date(dtAbertura);
            if (!isNaN(dataAberturaDate.getTime())) {
                var agora = new Date();
                var mesesDiferenca = (agora.getFullYear() - dataAberturaDate.getFullYear()) * 12 + (agora.getMonth() - dataAberturaDate.getMonth());
                if (mesesDiferenca < 6) {
                    alertasHTML += `<div style="color: #ffc107; font-weight: 500; margin-top: 0.5rem; padding: 0.75rem; background: rgba(255, 193, 7, 0.1); border-radius: 4px; border-left: 4px solid #ffc107;">⚠️ Alerta: Empresa aberta recentemente (menos de 6 meses de operação). Maior risco de capacidade operacional reduzida.</div>`;
                    isSuspicious = true;
                }
            }
        }

        if (capitalSocial === 0) {
            alertasHTML += `<div style="color: #ffc107; font-weight: 500; margin-top: 0.5rem; padding: 0.75rem; background: rgba(255, 193, 7, 0.1); border-radius: 4px; border-left: 4px solid #ffc107;">⚠️ Alerta: O capital social da empresa é R$ 0,00 ou não foi informado na Receita.</div>`;
            isSuspicious = true;
        } else if (capitalSocial < 10000) {
            alertasHTML += `<div style="color: #17a2b8; font-weight: 500; margin-top: 0.5rem; padding: 0.75rem; background: rgba(23, 162, 184, 0.1); border-radius: 4px; border-left: 4px solid #17a2b8;">ℹ️ Nota: O capital social da empresa é relativamente baixo (R$ ${capitalSocial.toLocaleString("pt-BR", { minimumFractionDigits: 2 })}).</div>`;
        }

        if (!alertasHTML) {
            alertasHTML = `<div style="color: #28a745; font-weight: 500; margin-top: 0.5rem; padding: 0.75rem; background: rgba(40, 167, 69, 0.1); border-radius: 4px; border-left: 4px solid #28a745;"> Análise concluída: Nenhum indício de anomalia óbvia encontrado nos dados básicos da Receita Federal.</div>`;
        }

        resultDiv.innerHTML = `
            <div style="display: flex; flex-direction: column; gap: 1rem;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                    <div>
                        <span style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; display: block; margin-bottom: 0.25rem;">Situação Cadastral</span>
                        <span style="font-weight: 600; color: ${statusColor}; font-size: 1rem;">${situacao}</span>
                    </div>
                    <div>
                        <span style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; display: block; margin-bottom: 0.25rem;">Data de Abertura</span>
                        <span style="font-weight: 500; font-size: 0.95rem;">${dtAberturaFormatada}</span>
                    </div>
                </div>
                
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                    <div>
                        <span style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; display: block; margin-bottom: 0.25rem;">Razão Social</span>
                        <span style="font-weight: 500; font-size: 0.95rem;">${data.razao_social || "-"}</span>
                    </div>
                    <div>
                        <span style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; display: block; margin-bottom: 0.25rem;">Capital Social</span>
                        <span style="font-weight: 500; font-size: 0.95rem;">R$ ${capitalSocial.toLocaleString("pt-BR", { minimumFractionDigits: 2 })}</span>
                    </div>
                </div>
                
                <div style="margin-top: 0.5rem;">
                    <h4 style="margin: 0 0 0.75rem 0; font-size: 1rem; color: var(--text-color);">Verificação de Suspeitas</h4>
                    ${alertasHTML}
                </div>
            </div>
            
            <div style="margin-top: 1.5rem; text-align: right;">
                <a href="https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp?cnpj=${cnpj}" target="_blank" style="font-size: 0.85rem; color: #007bff; text-decoration: none; font-weight: 500;">Abrir comprovante oficial da Receita ↗</a>
            </div>
        `;
        resultDiv.style.display = "block";

    } catch (e) {
        alert("Ocorreu um erro ao consultar a API: " + e.message);
    } finally {
        btn.textContent = "Consultar Receita Federal";
        btn.disabled = false;
    }
}
