(async () => {
  'use strict';

  const PLUGIN_NAME = 'bulkAddMissingTags';
  
  // Default configuration
  const defaultConfig = {
    autoCreateTags: true, // Automatically create tags if they don't exist
    requireConfirmation: true, // Ask for confirmation before bulk adding
    buttonPosition: 'bottom', // 'top' or 'bottom' of missing tags section
  };

  let pluginConfig = {};

  /**
   * Load plugin configuration from Stash
   */
  async function loadConfig() {
    const config = await csLib.getConfiguration(PLUGIN_NAME, {});
    pluginConfig = { ...defaultConfig, ...config };
  }

  /**
   * Find a tag by name or alias
   * @param {string} tagName - Name of the tag to find
   * @returns {Promise<string|null>} - Tag ID if found, null otherwise
   */
  async function findTagByName(tagName) {
    const tagFilter = {
      name: { value: tagName, modifier: 'EQUALS' },
      OR: { aliases: { value: tagName, modifier: 'EQUALS' } },
    };
    const findFilter = { per_page: -1 };
    const variables = { tag_filter: tagFilter, filter: findFilter };
    const query = `
      query FindTags($tag_filter: TagFilterType!, $filter: FindFilterType!) {
        findTags(filter: $filter, tag_filter: $tag_filter) {
          tags { id name }
        }
      }
    `;
    
    try {
      const result = await csLib.callGQL({ query, variables });
      const tags = result?.findTags?.tags;
      return tags && tags.length > 0 ? tags[0].id : null;
    } catch (error) {
      console.error(`Error finding tag "${tagName}":`, error);
      return null;
    }
  }

  /**
   * Create a new tag
   * @param {string} tagName - Name of the tag to create
   * @returns {Promise<string|null>} - Tag ID if created successfully, null otherwise
   */
  async function createTag(tagName) {
    const variables = { input: { name: tagName } };
    const query = `
      mutation CreateTag($input: TagCreateInput!) {
        tagCreate(input: $input) { id name }
      }
    `;
    
    try {
      const result = await csLib.callGQL({ query, variables });
      return result?.tagCreate?.id || null;
    } catch (error) {
      console.error(`Error creating tag "${tagName}":`, error);
      return null;
    }
  }

  /**
   * Get or create a tag ID
   * @param {string} tagName - Name of the tag
   * @returns {Promise<string|null>} - Tag ID if found/created, null otherwise
   */
  async function getOrCreateTagId(tagName) {
    let tagId = await findTagByName(tagName);
    
    if (!tagId && pluginConfig.autoCreateTags) {
      tagId = await createTag(tagName);
    }
    
    return tagId;
  }

  /**
   * Collect all missing tags from the tagger interface
   * @returns {Array<{name: string, element: HTMLElement}>} - Array of missing tag objects
   */
  function getMissingTags() {
    const missingTags = [];
    
    // Find all tag badges that have the create (+) button (these are missing tags)
    const tagBadges = document.querySelectorAll('.tag-item');
    
    tagBadges.forEach(badge => {
      // Check if this badge has a create button (faPlus icon)
      const createButton = badge.querySelector('button[title*="Create"], button[title*="create"]');
      if (createButton) {
        // Extract tag name from the badge
        // The tag name is typically in a text node or span before the buttons
        const tagNameElement = Array.from(badge.childNodes).find(
          node => node.nodeType === Node.TEXT_NODE && node.textContent.trim()
        );
        
        if (tagNameElement) {
          const tagName = tagNameElement.textContent.trim();
          if (tagName) {
            missingTags.push({ name: tagName, element: badge });
          }
        }
      }
    });
    
    return missingTags;
  }

  /**
   * Add all missing tags by clicking their create buttons
   */
  async function addAllMissingTags() {
    const missingTags = getMissingTags();
    
    if (missingTags.length === 0) {
      alert('No missing tags found.');
      return;
    }

    // Show confirmation if required
    if (pluginConfig.requireConfirmation) {
      const tagNames = missingTags.map(t => t.name).join(', ');
      const confirmMessage = `Add all ${missingTags.length} missing tag(s)?\n\nTags: ${tagNames}`;
      
      if (!confirm(confirmMessage)) {
        return;
      }
    }

    // Track progress
    let successCount = 0;
    let failCount = 0;

    // Process each missing tag
    for (const tag of missingTags) {
      try {
        // Find and click the create button on this tag
        const createButton = tag.element.querySelector('button[title*="Create"], button[title*="create"]');
        
        if (createButton) {
          createButton.click();
          successCount++;
          
          // Small delay to avoid overwhelming the UI
          await new Promise(resolve => setTimeout(resolve, 100));
        } else {
          console.warn(`Could not find create button for tag: ${tag.name}`);
          failCount++;
        }
      } catch (error) {
        console.error(`Error adding tag "${tag.name}":`, error);
        failCount++;
      }
    }

    // Show results
    if (successCount > 0) {
      console.log(`Successfully initiated addition of ${successCount} tag(s).`);
    }
    if (failCount > 0) {
      console.warn(`Failed to add ${failCount} tag(s).`);
    }
  }

  /**
   * Create and insert the "Add All Missing" button
   */
  function insertAddAllButton() {
    // Check if button already exists
    if (document.querySelector('.bulk-add-missing-tags-btn')) {
      return;
    }

    // Find the tags section - look for the area with missing tags
    const tagsSection = document.querySelector('.tag-item')?.parentElement;
    
    if (!tagsSection) {
      return; // No tags section found
    }

    // Check if there are any missing tags
    const missingTags = getMissingTags();
    if (missingTags.length === 0) {
      return; // No missing tags, don't show button
    }

    // Create the button
    const button = document.createElement('button');
    button.className = 'bulk-add-missing-tags-btn btn btn-primary';
    button.innerHTML = `
      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus-circle" viewBox="0 0 16 16" style="margin-right: 5px;">
        <path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/>
        <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4z"/>
      </svg>
      Add All Missing (${missingTags.length})
    `;
    button.title = 'Add all missing tags at once';
    
    // Add click handler
    button.onclick = async (e) => {
      e.preventDefault();
      e.stopPropagation();
      await addAllMissingTags();
    };

    // Insert button based on configuration
    if (pluginConfig.buttonPosition === 'top') {
      tagsSection.insertBefore(button, tagsSection.firstChild);
    } else {
      tagsSection.appendChild(button);
    }
  }

  /**
   * Initialize the plugin on tagger pages
   */
  async function initializePlugin() {
    await loadConfig();
    
    // Wait a bit for the tagger interface to fully load
    setTimeout(() => {
      insertAddAllButton();
      
      // Also set up a mutation observer to detect when new tags appear
      // This handles dynamic updates to the tagger interface
      const observer = new MutationObserver((mutations) => {
        // Check if we need to add/update the button
        insertAddAllButton();
      });
      
      // Observe the main content area for changes
      const mainContent = document.querySelector('.main');
      if (mainContent) {
        observer.observe(mainContent, {
          childList: true,
          subtree: true,
        });
      }
    }, 500);
  }

  /**
   * Set up plugin on relevant pages
   */
  function setupPlugin() {
    // Scene tagger page
    csLib.PathElementListener('/scenes/', '.tagger-container', initializePlugin);
    
    // Image tagger page  
    csLib.PathElementListener('/images/', '.tagger-container', initializePlugin);
    
    // Gallery tagger page
    csLib.PathElementListener('/galleries/', '.tagger-container', initializePlugin);
    
    // Also check for modal dialogs that contain the tagger
    csLib.waitForElement('.modal-content .tag-item', initializePlugin);
  }

  // Start the plugin
  setupPlugin();

  console.log('Bulk Add Missing Tags plugin loaded');
})();
