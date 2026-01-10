(function () {
  'use strict';

  const stash = window.stash || { setProgress: () => {} };
  console.log("🚀 BatchCreateAll ⚡ Ultra Speed Mode Initialized");

  const SHORT_DELAY = 10;
  let running = false;
  const createQueue = [];
  const tagQueue = [];

  const btnId = 'batch-create';
  const startLabel = 'Create All';
  const stopLabel = 'Stop';

  const btn = document.createElement("button");
  btn.id = btnId;
  btn.classList.add('btn', 'btn-primary', 'ml-3');
  btn.innerHTML = startLabel;
  btn.onclick = () => (running ? stop() : start());

  function getElementByXpath(path) {
    return document.evaluate(path, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
  }

  function sortElementChildren(container) {
    const children = Array.from(container.children);
    children.sort((a, b) => a.textContent.localeCompare(b.textContent));
    children.forEach(child => container.appendChild(child));
  }

  function placeButton() {
    const el = getElementByXpath("//button[text()='Scrape All']");
    if (el && !document.getElementById(btnId)) {
      const container = el.parentElement;
      container.appendChild(btn);
      sortElementChildren(container);
      el.classList.add('ml-3');
    }
  }

  const observer = new MutationObserver(placeButton);
  observer.observe(document.body, { childList: true, subtree: true });

  function start() {
    if (!confirm("Run ultra-fast Create + Tag?")) return;
    running = true;
    btn.innerHTML = stopLabel;
    btn.classList.replace('btn-primary', 'btn-danger');
    stash.setProgress(0);
    buildQueues();
    processInParallel();
  }

  function stop() {
    running = false;
    btn.innerHTML = startLabel;
    btn.classList.replace('btn-danger', 'btn-primary');
    stash.setProgress(0);
    createQueue.length = 0;
    tagQueue.length = 0;
  }

  // Helper function to call GraphQL
  async function callGQL(query, variables = {}) {
    const response = await fetch('/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, variables })
    });
    return response.json();
  }

  // Find tag by name
  async function findTagByName(tagName) {
    const query = `
      query FindTags($tag_filter: TagFilterType!, $filter: FindFilterType!) {
        findTags(filter: $filter, tag_filter: $tag_filter) {
          tags { id name }
        }
      }
    `;
    const variables = {
      tag_filter: {
        name: { value: tagName, modifier: 'EQUALS' },
        OR: { aliases: { value: tagName, modifier: 'EQUALS' } }
      },
      filter: { per_page: -1 }
    };
    const result = await callGQL(query, variables);
    const tags = result?.data?.findTags?.tags;
    return tags && tags.length > 0 ? tags[0].id : null;
  }

  // Create tag
  async function createTag(tagName) {
    const query = `
      mutation CreateTag($input: TagCreateInput!) {
        tagCreate(input: $input) { id name }
      }
    `;
    const variables = { input: { name: tagName } };
    try {
      const result = await callGQL(query, variables);
      return result?.data?.tagCreate?.id || null;
    } catch (error) {
      console.error(`Error creating tag "${tagName}":`, error);
      return null;
    }
  }

  // Get or create tag
  async function getOrCreateTag(tagName) {
    let tagId = await findTagByName(tagName);
    if (!tagId) {
      tagId = await createTag(tagName);
    }
    return tagId;
  }

  // Extract tag name from button context
  function getTagNameFromButton(btn) {
    // Look for the tag badge that contains this button
    const badge = btn.closest('.tag-item');
    if (!badge) return null;

    // Find text node with tag name
    for (const node of badge.childNodes) {
      if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
        return node.textContent.trim();
      }
    }
    return null;
  }

  function buildQueues() {
    createQueue.length = 0;
    tagQueue.length = 0;

    document.querySelectorAll('.btn-group').forEach(group => {
      const placeholder = group.querySelector('.react-select__placeholder');
      if (!placeholder) return;

      const txt = placeholder.textContent.trim();
      if (txt === 'Select Performer' || txt === 'Select Studio') {
        const button = group.querySelector('button.btn.btn-secondary');
        if (button && button.textContent.trim() === 'Create' && !button.disabled) {
          createQueue.push(button);
        }
      }
    });

    // Collect tag buttons with their metadata
    document.querySelectorAll('.search-item button.minimal.ml-2.btn.btn-primary')
      .forEach(btn => {
        const tagName = getTagNameFromButton(btn);
        tagQueue.push({ button: btn, tagName });
      });
  }

  async function processInParallel() {
    const total = createQueue.length + tagQueue.length;
    let processed = 0;

    const processCreate = async () => {
      while (running && createQueue.length) {
        const btn = createQueue.shift();
        if (!btn) break;
        btn.click();

        await delay(SHORT_DELAY); // let modal open
        const saveBtn = document.querySelector('.ModalFooter.modal-footer button.btn.btn-primary');
        if (saveBtn) saveBtn.click();
        processed++;
        stash.setProgress((processed / total) * 100);
        await delay(SHORT_DELAY);
      }
    };

    const processTags = async () => {
      // PHASE 1: Pre-create all unique tags to avoid duplicate creation errors
      const uniqueTagNames = [...new Set(tagQueue.map(item => item.tagName).filter(Boolean))];

      if (uniqueTagNames.length > 0) {
        console.log(`🏷️ Pre-creating ${uniqueTagNames.length} unique tag(s)...`);
        for (const tagName of uniqueTagNames) {
          if (!running) break;
          try {
            const tagId = await getOrCreateTag(tagName);
            if (tagId) {
              console.log(`✅ Tag ready: "${tagName}" (ID: ${tagId})`);
            }
          } catch (error) {
            console.error(`❌ Error with tag "${tagName}":`, error);
          }
        }

        // Wait for UI to update (create buttons → link buttons)
        await delay(500);
      }

      // PHASE 2: Click the buttons to link tags to items
      while (running && tagQueue.length) {
        const item = tagQueue.shift();
        if (!item || !item.button) break;
        item.button.click();
        processed++;
        stash.setProgress((processed / total) * 100);
        await delay(SHORT_DELAY);
      }
    };

    await Promise.all([processCreate(), processTags()]);
    stop();
  }

  function delay(ms) {
    return new Promise(res => setTimeout(res, ms));
  }
})();
