// Multi-season NBA ratings viewer
// Seasons: 2024 (2023-24), 2026 (2025-26)

window.addEventListener("DOMContentLoaded", () => {
  const seasonSelect = document.getElementById("season-select");
  const sortSelect = document.getElementById("sort-select");
  const filterInput = document.getElementById("filter-input");
  const tableBody = document.getElementById("rankings-body");
  const statusEl = document.getElementById("status");

  const seasonFiles = {
    "2024": "ratings_2024.json",
    "2026": "ratings_2026.json",
  };

  let originalData = [];
  let currentData = [];
  let currentSeason = seasonSelect.value;

  async function loadSeason(season) {
    const file = seasonFiles[season];
    currentSeason = season;

    try {
      statusEl.textContent = `Loading ${season}…`;
      const response = await fetch(file, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const data = await response.json();

      originalData = data
        .map(r => ({ team: r.team, rating: Number(r.rating) }))
        .sort((a, b) => b.rating - a.rating);

      currentData = [...originalData];
      renderTable();
      statusEl.textContent = `Loaded ${currentData.length} teams for ${season}.`;
    } catch (e) {
      statusEl.textContent = `Error loading season ${season}: ${e.message}`;
      console.error(e);
    }
  }

  function renderTable() {
    tableBody.innerHTML = "";
    currentData.forEach((row, i) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${i + 1}</td>
        <td>${row.team}</td>
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
      rows = rows.filter(r => r.team.toUpperCase().includes(filter));
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

  seasonSelect.addEventListener("change", e => {
    loadSeason(e.target.value);
  });

  sortSelect.addEventListener("change", applySortAndFilter);
  filterInput.addEventListener("input", applySortAndFilter);

  loadSeason(currentSeason);
});
