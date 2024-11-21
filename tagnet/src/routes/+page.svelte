<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";

  const markdownURI = "data:image/jpeg;base64,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";

  let currentlyGenerating = new Set<integer>;

  let name = $state("");
  let color = $state("#8f008f");

  let files = $state([]);
  let tags = $state([]);

  let focusedFile = $state(null);
  let focusedTag = $state(null);
  let selectedFiles = $state([]);

  let searchElement;
  let searchBar = $state("");
  let searchBarColor = $state("#0f0f0f98");

  let editTagName = $state("");
  let editTagColor = $state("");
  let editTagTags = $state([]);
  let editTagSubags = $state([]);

  let editFileTags = $state([]);

  let editAdding = $state(null);

  let selectedTags = $state([]);

  let displaySize = $state("small");
  let smallCache = $state({
    0: markdownURI,
  })
  let mediumCache = $state({
    0: markdownURI,
  })
  let bigCache = $state({
    0: markdownURI,
  })

  async function loadInitial() {
    files = await invoke("all_files", { });
    tags = await invoke("all_tags", { });
  }

  setTimeout(loadInitial, 10);

  async function focusTag(tag) {
    // TODO: Make this an union of literals.
    if (editAdding == "tags") {
      await invoke("tag_tag", { tagId: tag.id, subtagId: focusedTag.id });
      editTagTags = await invoke("tags_for_subtag", { subtagId: focusedTag.id });
      editAdding = null;
      return;
    } else if (editAdding == "subtags") {
      await invoke("tag_tag", { tagId: focusedTag.id, subtagId: tag.id });
      editTagSubags = await invoke("subtags_for_tag", { tagId: focusedTag.id });
      editAdding = null;
      return;
    } else if (editAdding == "file_tags") {
      await invoke("tag_file", { fileId: focusedFile.id, tagId: tag.id });
      editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });

      if (selectedFiles.length > 0) {
        selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });
      }

      editAdding = null;
      return;
    } else if (editAdding == "selected_tags") {
      await invoke("tag_selected", { selectedIds: selectedFiles, tagId: tag.id });
      selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });

      if (focusedFile) {
        editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });
      }

      editAdding = null;
      return;
    }

    focusedTag = tag;
    editTagName = tag.name;
    editTagColor = tag.color;
    editTagTags = await invoke("tags_for_subtag", { subtagId: tag.id });
    editTagSubags = await invoke("subtags_for_tag", { tagId: tag.id });
  }

  async function focusFile(event: Event, file) {
    if (event.shiftKey) {
      const lastId = selectedFiles.at(-1);

      var started = false;
      for (let fileId of files.map((file) => file.id)) {
        if (started) {
          if (!selectedFiles.includes(fileId)) {
            selectedFiles.push(fileId);
          }
            console.log("fileId: " + fileId);

          // We are at the end of our selection
          if (fileId === lastId || fileId == file.id) {
            break;
          }
        } else {
          // We are at the sart of our selection
          if (fileId === lastId || fileId == file.id) {
            if (!selectedFiles.includes(fileId)) {
              selectedFiles.push(fileId);
            }

            console.log("Started!");

            started = true;
          }
        }
      }


      // TODO: This causes some flickering. Better to unbind the original handler.
      document.getSelection().removeAllRanges();

      selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });
      return;
    }

    if (event.ctrlKey) {
      if (selectedFiles.includes(file.id)) {
        selectedFiles = selectedFiles.filter(id => id !== file.id);
      } else {
        selectedFiles.push(file.id);
      }

      selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });
      return;
    }

    focusedFile = file;
    editFileTags = await invoke("tags_for_file", { fileId: file.id });
  }

  async function untagFile(tag) {
    await invoke("untag_file", { fileId: focusedFile.id, tagId: tag.id });
    editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });

    if (selectedFiles.length > 0) {
      selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });
    }
  }

  async function untagTag(tag) {
    await invoke("untag_tag", { tagId: tag.id, subtagId: focusedTag.id });
    editTagTags = await invoke("tags_for_subtag", { subtagId: focusedTag.id });
  }

  async function untagSubtag(tag) {
    await invoke("untag_tag", { tagId: focusedTag.id, subtagId: tag.id });
    editTagSubags = await invoke("subtags_for_tag", { tagId: focusedTag.id });
  }

  async function untagSelected(tag) {
    await invoke("untag_selected", { selectedIds: selectedFiles, tagId: tag.id });
    selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });

    if (focusedFile) {
      editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });
    }
  }

  async function addTag(event: Event) {
    event.preventDefault();
    await invoke("add_tag", { name, color });
    tags = await invoke("all_tags", { });
    name = "";
  }

  async function removeTag(tag) {
    await invoke("remove_tag", { tagId: tag.id });
    focusedTag = null;
    tags = await invoke("all_tags", { });

    if (focusedFile) {
      editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });
    }

    if (selectedFiles.length > 0) {
      selectedTags = await invoke("tags_for_selected", { selectedIds: selectedFiles });
    }
  }

  async function updateTag(event: Event) {
    event.preventDefault();
    await invoke("update_tag", { tagId: focusedTag.id, name: editTagName, color: editTagColor });
    // TODO: Don't fetch all tags
    tags = await invoke("all_tags", { });
  }

  // Search bar.
  $effect(async () => {
    invoke("filter_files", { searchBar }).then(newFiles => {
      if (newFiles.length > 0) {
        files = newFiles;
        searchBarColor = "#0f0f0f98";
      } else {
        searchBarColor = "#D4313198";
      }
    });
  });

  function onKeyDown(event: Event) {
    switch(event.keyCode) {
      case 191:
        console.log("Focus thign;");
        searchElement.focus();
        searchElement.select();
        event.preventDefault();
        break;
    }
  }

  function getEntryClass() {
    if (displaySize == "small") {
      return "file-entry-small";
    } else if (displaySize == "medium") {
      return "file-entry-medium";
    } else if (displaySize == "big") {
      return "file-entry-big";
    }
  }

  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function loadPreview(file, previewSize) {
    while (true) {
      const generatePreview = !file.preview_id;

      console.log(file.preview_id + " -> " + generatePreview);

      // If the file already has a preview, just fetch it.
      if (generatePreview) {
        if (currentlyGenerating.size > 3) {
          // Wait and retry later
          await delay(1000);
          continue;
        }

        if (currentlyGenerating.has(file.id)) {
          // Preview is already beeing generated at the moment.
          await delay(1000);
          continue;
        }

        // Proceed when fetching limit allows
        currentlyGenerating.add(file.id);
        console.log("Generating preview for " + file.display_name + " (size " + previewSize + ")");
      }

      try {
        // Generate and cache preview
        const result = await invoke("get_preview", { file, previewSize });
        return result;
      } catch (err) {
        console.error(`Failed to load preview for ${file.id}:`, err);
      } finally {
        // Ensure cleanup happens even if an error occurs
        currentlyGenerating.delete(file.id);
      }

      break; // Exit the loop after fetching or handling an error
    }
  }

  function getPreview(file) {
    if (file.has_preview) {
      let currentCache;

      if (displaySize == "small") {
        currentCache = smallCache;
      } else if (displaySize == "medium") {
        currentCache = mediumCache;
      } else if (displaySize == "big") {
        currentCache = bigCache;
      }

      if (!file.preview_id || !currentCache[file.preview_id]) {
        loadPreview(file, displaySize).then(([preview_id, preview]) => {
          console.log("Setting preview for: " + preview_id);
          // TODO: Maybe only conditionally.
          file.preview_id = preview_id;
          currentCache[file.preview_id] = preview;
        });
      }

      return currentCache[file.preview_id];
    } else {
      return markdownURI;
    }
  }
</script>

<main class="main-window">
  <div style="width: clamp(200px, 30%, 400px); min-width: clamp(200px, 30%, 400px)">
    <div class="sub-window">
      <h4>Tags</h4>
      <form onsubmit={addTag}>
        <!-- FIX THIS width -->
        <input id="new-tag-name" placeholder="Tag Name" style="width: calc(100% - 3em);" bind:value={name} />
      </form>

      <div>
        {#each tags as tag}
          <div class="tag" style="background: {tag.color}" onclick={() => focusTag(tag)}>{tag.name}</div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={addTag}>+</div>
      </div>
    </div>

    {#if focusedTag}
      <div class="sub-window">
        <h4>Edit Tag</h4>
        <form onsubmit={updateTag}>
          <!-- FIX THIS width -->
          <input id="new-tag-name" placeholder="Tag Name" style="width: calc(100% - 3em);" bind:value={editTagName} />
          <!-- FIX THIS width -->
          <input id="new-tag-color" placeholder="Tag Color" style="width: calc(100% - 3em);" bind:value={editTagColor} />
          <button type="submit">Update tag</button>
        </form>
        <h5>Tags</h5>
        {#each editTagTags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="untag-button" onclick={() => untagTag(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={() => editAdding = "tags"}>+</div>
        <h5>Subtags</h5>
        {#each editTagSubags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="untag-button" onclick={() => untagSubtag(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={() => editAdding = "subtags"}>+</div>

        <h5>Danger Zone</h5>
        <div class="remove-tag-button" onclick={() => removeTag(focusedTag)}>Remove Tag</div>
      </div>
    {/if}

    {#if focusedFile}
      <div class="sub-window">
        <h4>Edit File</h4>

        <h5>Path: {focusedFile.path}</h5>
        <h5>Modified: {focusedFile.last_modified}</h5>
        <h5>Size: {focusedFile.content_length}</h5>
        <h5>Type: {focusedFile.content_type}</h5>

        <h5>Tags</h5>
        {#each editFileTags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="untag-button" onclick={() => untagFile(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={() => editAdding = "file_tags"}>+</div>
      </div>
    {/if}

    {#if selectedFiles.length > 0}
      <div class="sub-window">
        <h4>Selected Files</h4>

        <h5>Selected Files: {selectedFiles.length}</h5>

        <h5>Tags</h5>
        {#each selectedTags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="untag-button" onclick={() => untagSelected(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={() => editAdding = "selected_tags"}>+</div>

        <h5>Selection</h5>
        <div class="remove-tag-button" onclick={() => selectedFiles = []}>Remove Selection</div>
      </div>
    {/if}

    {#if editAdding}
      <h4>Adding {editAdding}</h4>
      <button onclick={() => editAdding = null}>Cancel</button>
    {/if}
  </div>

  <div style="flex-grow: 1; margin-left: 1rem;">
    <div class="sub-window">
      <h4>Files</h4>

      <label>
      <input
      type="radio"
      bind:group={displaySize}
      value="small"
      />
      Small
      </label>
      <label>
      <input
      type="radio"
      bind:group={displaySize}
      value="medium"
      />
      Medium
      </label>
      <label>
      <input
      type="radio"
      bind:group={displaySize}
      value="big"
      />
      Big
      </label>

      <!-- FIX THIS width -->
      <input bind:this={searchElement} id="test-input" placeholder="Tag ID" style="width: calc(100% - 3em); background-color: {searchBarColor}" bind:value={searchBar} />

      <div style="overflow-y: scroll; max-height: calc(100vh - 106px);">
        {#each files as file}
          <!-- FIX: This is horrible for performance -->
          {#if selectedFiles.length > 0 && selectedFiles.includes(file.id)}
              <div class={getEntryClass()} onclick={(event) => focusFile(event, file)} style="border-color: #38DBFF;">
                <img src={getPreview(file)}/>
                <!-- {file.display_name} -->
              </div>
          {:else}
              <div class={getEntryClass()} onclick={(event) => focusFile(event, file)}>
                <img src={getPreview(file)}/>
                <!-- {file.display_name} -->
              </div>
          {/if}
        {/each}
      </div>
    </div>
  </div>
</main>

<svelte:window on:keydown={onKeyDown}/>

<style>

:root {
  font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
  font-size: 12px;
  line-height: 24px;
  font-weight: 400;
  cursor: crosshair;

  color: #0f0f0f;
  background-color: #f6f6f6;

  height: 100%;
  overflow: hidden;

  font-synthesis: none;
  text-rendering: optimizeLegibility;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  -webkit-text-size-adjust: 100%;
}

.main-window {
  display: flex;
  flex-direction: row;
  padding: 0.4rem;
}

.sub-window {
  background-color: #444444;
  border-radius: 0.5rem;
  padding: 0.5rem;
  margin-bottom: 1rem;
}

.tag {
  display: inline-block;
  cursor: zoom-in;
  font-size: 1rem;
  border-radius: 0.5rem;
  padding: 0 0.8rem;
  margin: 0.2rem;
}

.edit-tag {
  display: inline-block;
  margin: 0.2rem;
  border-radius: 0.5rem;
}

.edit-tag-text {
  display: inline-block;
  cursor: zoom-in;
  font-size: 1rem;
  height: 100%;
  padding: 0 0.8rem;
}

.untag-button {
  display: inline-block;
  cursor: pointer;
  background-color: rgba(0, 0, 0, 0.2);
  border-radius: 0.5rem;
  font-size: 1rem;
  padding: 0 0.8rem;
}

.remove-tag-button {
  background-color: red;
  font-size: 1rem;
  border-radius: 0.5rem;
  padding: 0 0.8rem;
  margin: 0.2rem;
}

.file-entry-small {
  display: inline-block;
  background-color: black;
  color: #aaaaaa;
  border-radius: 1rem;
  border: 2px solid;
  border-color: black;
  width: 60px;
  height: 60px;
  font-size: 1rem;
  cursor: pointer;
  margin: 2px;
  overflow: clip;
}

.file-entry-small > img {
  width: 60px;
  height: 60px;
  object-fit: cover;
}

.file-entry-medium {
  display: inline-block;
  background-color: black;
  color: #aaaaaa;
  border-radius: 1rem;
  border: 2px solid;
  border-color: black;
  width: 100px;
  height: 100px;
  font-size: 1rem;
  cursor: pointer;
  margin: 2px;
  overflow: clip;
}

.file-entry-medium > img {
  width: 100px;
  height: 100px;
  object-fit: cover;
}

.file-entry-big {
  display: inline-block;
  background-color: black;
  color: #aaaaaa;
  border-radius: 1rem;
  border: 2px solid;
  border-color: black;
  width: 150px;
  height: 150px;
  font-size: 1rem;
  cursor: pointer;
  margin: 2px;
  overflow: clip;
}

.file-entry-big > img {
  width: 150px;
  height: 150px;
  object-fit: cover;
}

h4 {
  text-align: center;
  font-size: 1.2rem;
  margin-top: 0;
  margin-bottom: 0.5rem;
}

h5 {
  margin-top: 0.2rem;
  margin-bottom: 0;
  color: #bbbbbb;
}

input {
  margin-bottom: 0.5rem;
}

input,
button {
  border-radius: 8px;
  border: 1px solid transparent;
  padding: 0.6em 1.2em;
  font-size: 1em;
  font-weight: 500;
  font-family: inherit;
  color: #0f0f0f;
  background-color: #ffffff;
  transition: border-color 0.25s;
  box-shadow: 0 2px 2px rgba(0, 0, 0, 0.2);
}

button {
  cursor: pointer;
}

button:hover {
  border-color: #396cd8;
}
button:active {
  border-color: #396cd8;
  background-color: #e8e8e8;
}

input,
button {
  outline: none;
}

@media (prefers-color-scheme: dark) {
  :root {
    color: #f6f6f6;
    background-color: #2f2f2f;

    font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
    font-size: 12px;
    line-height: 24px;
    font-weight: 400;
    cursor: crosshair;

    height: 100%;
    overflow: hidden;

    font-synthesis: none;
    text-rendering: optimizeLegibility;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    -webkit-text-size-adjust: 100%;
  }

  input,
  button {
    color: #ffffff;
    background-color: #0f0f0f98;
  }

  button:active {
    background-color: #0f0f0f69;
  }
}

</style>
