<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";

  let name = $state("");
  let color = $state("#8f008f");

  let files = $state([]);
  let tags = $state([]);

  let focusedFile = $state(null);
  let focusedTag = $state(null);

  let searchBar = $state("");

  let editTagName = $state("");
  let editTagColor = $state("");
  let editTagTags = $state([]);
  let editTagSubags = $state([]);

  let editFileTags = $state([]);

  let editAdding = $state(null);

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
      editAdding = null;
      return;
    }

    focusedTag = tag;
    editTagName = tag.name;
    editTagColor = tag.color;
    editTagTags = await invoke("tags_for_subtag", { subtagId: tag.id });
    editTagSubags = await invoke("subtags_for_tag", { tagId: tag.id });
  }

  async function focusFile(file) {
    focusedFile = file;
    editFileTags = await invoke("tags_for_file", { fileId: file.id });
  }

  async function untagFile(tag) {
    await invoke("untag_file", { fileId: focusedFile.id, tagId: tag.id });
    editFileTags = await invoke("tags_for_file", { fileId: focusedFile.id });
  }

  async function untagTag(tag) {
    await invoke("untag_tag", { tagId: tag.id, subtagId: focusedTag.id });
    editTagTags = await invoke("tags_for_subtag", { subtagId: focusedTag.id });
  }

  async function untagSubtag(tag) {
    await invoke("untag_tag", { tagId: focusedTag.id, subtagId: tag.id });
    editTagSubags = await invoke("subtags_for_tag", { tagId: focusedTag.id });
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
  }

  async function updateTag(event: Event) {
    event.preventDefault();
    await invoke("update_tag", { tagId: focusedTag.id, name: editTagName, color: editTagColor });
    // TODO: Don't fetch all tags
    tags = await invoke("all_tags", { });
  }

  // TODO: Remove function
  async function testme(event: Event) {
    event.preventDefault();

    if (searchBar.length == 0) {
        files = await invoke("all_files", { });
    } else {
        files = await invoke("files_for_tag", { tag: searchBar });
    }
  }
</script>

<main class="main-window">
  <div style="width: 30%; min-width: 30%">
    <div class="sub-window">
      <h4>TAG MANAGMENT</h4>
      <form onsubmit={addTag}>
        <!-- FIX THIS width -->
        <input id="new-tag-name" placeholder="Tag name" style="width: calc(100% - 3em);" bind:value={name} />
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
        <h4>EDIT TAG</h4>
        <form onsubmit={updateTag}>
          <!-- FIX THIS width -->
          <input id="new-tag-name" placeholder="Tag name" style="width: calc(100% - 3em);" bind:value={editTagName} />
          <!-- FIX THIS width -->
          <input id="new-tag-color" placeholder="Tag color" style="width: calc(100% - 3em);" bind:value={editTagColor} />
          <button type="submit">Update tag</button>
        </form>
        <h5>TAGS</h5>
        {#each editTagTags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="remove-tag" onclick={() => untagTag(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={editAdding = "tags"}>+</div>
        <h5>SUBTAGS</h5>
        {#each editTagSubags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="remove-tag" onclick={() => untagSubtag(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={editAdding = "subtags"}>+</div>

        <h5>DANGER ZONE</h5>
        <button onclick={() => removeTag(focusedTag)} style="background-color: red">Remove Tag</button>
      </div>
    {/if}

    {#if focusedFile}
      <div class="sub-window">
        <h4>EDIT FILE</h4>

        <h5>Path: {focusedFile.path}</h5>
        <h5>Modified: {focusedFile.last_modified}</h5>
        <h5>Size: {focusedFile.content_length}</h5>
        <h5>Type: {focusedFile.content_type}</h5>

        <h5>TAGS</h5>
        {#each editFileTags as tag}
          <div class="edit-tag" style="background: {tag.color}">
            <div class="edit-tag-text" onclick={() => focusTag(tag)}>
              {tag.name}
            </div>
            <div class="remove-tag" onclick={() => untagFile(tag)}>-</div>
          </div>
        {/each}
        <div class="tag" style="background-color: #888888" onclick={() => editAdding = "file_tags"}>+</div>
      </div>
    {/if}

    {#if editAdding}
      <h4>Adding {editAdding}</h4>
      <button onclick={editAdding = null}>Cancel</button>
    {/if}
  </div>

  <div style="flex-grow: 1; margin-left: 1rem;">
    <div class="sub-window">
      <h4>FILE MANAGMENT</h4>
      <form onsubmit={testme}>
        <!-- FIX THIS width -->
        <input id="test-input" placeholder="Tag Id" style="width: calc(100% - 3em);" bind:value={searchBar} />
        <button type="submit">Get files</button>
      </form>

      {#each files as file}
        <div class="file-entry" onclick={() => focusFile(file)}>{file.display_name}</div>
      {/each}
    </div>
  </div>
</main>

<style>

:root {
  font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
  font-size: 12px;
  line-height: 24px;
  font-weight: 400;

  color: #0f0f0f;
  background-color: #f6f6f6;

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

.remove-tag {
  display: inline-block;
  cursor: pointer;
  background-color: rgba(0, 0, 0, 0.2);
  border-radius: 0.5rem;
  font-size: 1rem;
  padding: 0 0.8rem;
}

.file-entry {
  display: inline-block;
  background-color: black;
  color: #aaaaaa;
  border-radius: 1rem;
  width: 100px;
  height: 100px;
  font-size: 1rem;
  cursor: pointer;
  padding: 10px;
  margin: 2px;
  overflow: clip;
}

h4 {
  margin-top: 0.5rem;
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
