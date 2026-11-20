"use strict";

const DEFAULT_COMPANY = "SYN-001550";
const lookup = document.querySelector("#lookup");
const registrationInput = document.querySelector("#registration");
const workspace = document.querySelector("#workspace");
const notice = document.querySelector("#notice");
const resultNotice = document.querySelector("#results-notice");
const results = document.querySelector("#results");
const resultLimit = document.querySelector("#result-limit");
const profileRegistration = document.querySelector("#profile-registration");
const cardTemplate = document.querySelector("#card");
const profileName = document.querySelector("#profile-name");
const scoreFormat = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
let activeRequest;

function companyLink(registration) {
  return "/?" + new URLSearchParams({ company: registration });
}

function showNotice(element, text, error = false) {
  element.textContent = text;
  element.toggleAttribute("data-error", error);
  element.hidden = text === "";
}

async function getJSON(url, signal) {
  const response = await fetch(url, { signal });
  if (response.ok) return response.json();
  if (response.status === 404) throw new Error("No active company was found for that registration number.");
  throw new Error("The registry is unavailable right now. Please try again.");
}

function showProfile(company) {
  profileName.textContent = company.name || "Company name unavailable";
  profileRegistration.textContent = company.registration_number;
  document.querySelector("#profile-description").textContent =
    company.description || "No business description is available.";
  document.querySelector("#profile-meta").textContent =
    [company.industry, company.state].filter(Boolean).join(" · ") || "Industry and state unavailable";
}

function showRecommendations(cards) {
  const items = cards.map(company => {
    const item = cardTemplate.content.cloneNode(true);
    const link = item.querySelector("a");
    link.href = companyLink(company.registration_number);
    link.dataset.company = company.registration_number;
    item.querySelector(".card-name").textContent = company.name || "Company name unavailable";
    item.querySelector(".card-details").textContent =
      [company.industry, company.state].filter(Boolean).join(" · ") || "Industry and state unavailable";
    item.querySelector(".identifier").textContent = company.registration_number;
    const score = company.similarity_percent;
    item.querySelector(".similarity strong").textContent = `${scoreFormat.format(score)}%`;
    item.querySelector("meter").value = score;
    return item;
  });
  results.replaceChildren(...items);
  results.hidden = cards.length === 0;
  showNotice(resultNotice, cards.length ? "" : "No recommendations are available for this company.");
}

function beginRequest() {
  activeRequest?.abort();
  activeRequest = new AbortController();
  return activeRequest.signal;
}

async function loadRecommendations(registration, signal) {
  results.replaceChildren();
  results.hidden = true;
  showNotice(resultNotice, "Loading recommendations…");
  try {
    const path = "/companies/" + encodeURIComponent(registration) + "/similar";
    const query = new URLSearchParams({ limit: resultLimit.value });
    const { recommendations } = await getJSON(path + "?" + query, signal);
    if (!signal.aborted) showRecommendations(recommendations);
  } catch {
    if (!signal.aborted) showNotice(resultNotice, "Could not load recommendations. Please try again.", true);
  }
}

async function loadCompany(registration, { navigate = false, focus = false } = {}) {
  const signal = beginRequest();
  registrationInput.value = registration;
  registrationInput.setCustomValidity("");
  if (navigate && location.search !== new URL(companyLink(registration), location.origin).search) {
    history.pushState(null, "", companyLink(registration));
  }

  workspace.hidden = true;
  showNotice(notice, "Loading company…");
  const path = "/companies/" + encodeURIComponent(registration);
  try {
    const company = await getJSON(path, signal);
    if (signal.aborted) return;
    showProfile(company);
    showNotice(notice, "");
    workspace.hidden = false;
    if (focus) profileName.focus();
  } catch (error) {
    if (!signal.aborted) {
      showNotice(notice, error instanceof TypeError ? "Unable to reach the registry. Please try again." : error.message, true);
    }
    return;
  }

  await loadRecommendations(registration, signal);
}

resultLimit.addEventListener("change", () => {
  void loadRecommendations(profileRegistration.textContent, beginRequest());
});

lookup.addEventListener("submit", event => {
  event.preventDefault();
  const value = registrationInput.value.trim();
  if (!value) {
    registrationInput.setCustomValidity("Enter a registration number.");
    registrationInput.reportValidity();
    return;
  }
  const registration = /^syn-\d+$/i.test(value) ? value.toUpperCase() : value;
  void loadCompany(registration, { navigate: true, focus: true });
});

registrationInput.addEventListener("input", () => registrationInput.setCustomValidity(""));
document.addEventListener("click", event => {
  const link = event.target.closest("a[data-company]");
  if (!link || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  void loadCompany(link.dataset.company, { navigate: true, focus: true });
});

function loadFromAddress() {
  const registration = new URLSearchParams(location.search).get("company")?.trim();
  void loadCompany(registration || DEFAULT_COMPANY);
}
window.addEventListener("popstate", loadFromAddress);
loadFromAddress();
