// Simple ratings viewer that reads canonical data/ratings_current.json
window.addEventListener("DOMContentLoaded", () => {
  const seasonSelect = document.getElementById("season-select");
  const sortSelect = document.getElementById("sort-select");
  const filterInput = document.getElementById("filter-input");
  const tableBody = document.getElementById("rankings-body");
  const statusEl = document.getElementById("status");

  const TEAM_NAMES = {
    ATL: "Atlanta Hawks",
    BOS: "Boston Celtics",
    BKN: "Brooklyn Nets",
    CHA: "Charlotte Hornets",
    CHI: "Chicago Bulls",
    CLE: "Cleveland Cavaliers",
    DAL: "Dallas Mavericks",
    DEN: "Denver Nuggets",
    DET: "Detroit Pistons",
    GSW: "Golden State Warriors",
    HOU: "Houston Rockets",
    IND: "Indiana Pacers",
    LAC: "Los Angeles Clippers",
    LAL: "Los Angeles Lakers",
    MEM: "Memphis Grizzlies",
    MIA: "Miami Heat",
    MIL: "Milwaukee Bucks",
    MIN: "Minnesota Timberwolves",
    NOP: "New Orleans Pelicans",
    NYK: "New York Knicks",
    OKC: "Oklahoma City Thunder",
    ORL: "Orlando Magic",
    PHI: "Philadelphia 76ers",
    PHX: "Phoenix Suns",
    POR: "Portland Trail Blazers",
    SAC: "Sacramento Kings",
    SAS: "San Antonio Spurs",
    TOR: "Toronto Raptors",
    UTA: "Utah Jazz",
    WAS: "Washington Wizards",
  };

  let originalData = [];
  let currentData = [];

  async function loadRatings() {
    const file = "data/ratings_current.json";
    try {
      statusEl.textContent = "Loading...";
      tableBody.innerHTML = "";

      const response = await fetch(file, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const raw = await response.json();
      const data = Array.isArray(raw) ? raw : Array.isArray(raw.ratings) ? raw.ratings : [];

      originalData = data
        .map((r) => ({ team: r.team, rating: Number(r.rating) }))
        .sort((a, b) => b.rating - a.rating);

      currentData = [...originalData];
      renderTable();
      statusEl.textContent = `Loaded ${currentData.length} teams.`;
    } catch (e) {
      statusEl.textContent = `Error loading ratings: ${e.message}`;
      console.error(e);
    }
  }

  function renderTable() {
    tableBody.innerHTML = "";
    currentData.forEach((row, i) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${i + 1}</td>
        <td>${TEAM_NAMES[row.team] ?? row.team}</td>
        <td class="numeric">${row.rating.toFixed(3)}</td>
      `;
      tableBody.appendChild(tr);
    });
  }

  function applySortAndFilter() {
    const sort = sortSelect.value;
    const filter = filterInput.value.trim().toUpperCase();

    let rows = [...originalData];

    if (filter) {
      rows = rows.filter((r) => r.team.toUpperCase().includes(filter));
    }

    switch (sort) {
      case "rating_asc":
        rows.sort((a, b) => a.rating - b.rating);
        break;
      case "team_asc":
        rows.sort((a, b) => a.team.localeCompare(b.team));
        break;
      case "team_desc":
        rows.sort((a, b) => b.team.localeCompare(a.team));
        break;
      default:
        rows.sort((a, b) => b.rating - a.rating);
    }

    currentData = rows;
    renderTable();
  }

  sortSelect.addEventListener("change", applySortAndFilter);
  filterInput.addEventListener("input", applySortAndFilter);
  seasonSelect.addEventListener("change", applySortAndFilter);

  loadRatings();
});
