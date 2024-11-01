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
    // TODO: Make this an enum.
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

  <div style="column: 1; row: 1;">
    <div class="tag-window">
      <h1>TAG MANAGMENT</h1>
      <form onsubmit={addTag}>
        <!-- FIX THIS width -->
        <input id="new-tag-name" placeholder="Tag name" style="width: calc(100% - 3em);" bind:value={name} />
      </form>

      <div class="tag-field">
        {#each tags as tag}
          <button class="tag" style="background: {tag.color}" onclick={() => focusTag(tag)}>{tag.name}</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={addTag}>+</button>
      </div>
    </div>

    {#if focusedTag}
      <div class="edit-tag-window">
        <h1>EDIT TAG</h1>
        <form onsubmit={updateTag}>
          <!-- FIX THIS width -->
          <input id="new-tag-name" placeholder="Tag name" style="width: calc(100% - 3em);" bind:value={editTagName} />
          <!-- FIX THIS width -->
          <input id="new-tag-color" placeholder="Tag color" style="width: calc(100% - 3em);" bind:value={editTagColor} />
          <button type="submit">Update tag</button>
        </form>
        <h1>TAGS</h1>
        {#each editTagTags as tag}
          <button class="edit-tag" style="background: {tag.color}" onclick={() => focusTag(tag)}>{tag.name}</button>
          <button class="remove-tag" style="background: #888888" onclick={() => untagTag(tag)}>-</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={editAdding = "tags"}>+</button>
        <h1>SUBTAGS</h1>
        {#each editTagSubags as tag}
          <button class="edit-tag" style="background: {tag.color}" onclick={() => focusTag(tag)}>{tag.name}</button>
          <button class="remove-tag" style="background: #888888" onclick={() => untagSubtag(tag)}>-</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={editAdding = "subtags"}>+</button>

        <h1>DANGER ZONE</h1>
        <button onclick={() => removeTag(focusedTag)} style="background-color: red">Remove Tag</button>
      </div>
    {/if}

    {#if focusedFile}
      <div class="edit-file-window">
        <h1>EDIT FILE</h1>
        <h1>Focusing file: {focusedFile.path}</h1>
        <h1>TAGS</h1>
        {#each editFileTags as tag}
          <button class="edit-tag" style="background: {tag.color}" onclick={() => focusTag(tag)}>{tag.name}</button>
          <button class="remove-tag" style="background: #888888" onclick={() => untagFile(tag)}>-</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={() => editAdding = "file_tags"}>+</button>
      </div>
    {/if}

    {#if editAdding}
      <h1>Adding {editAdding}</h1>
      <button onclick={editAdding = null}>Cancel</button>
    {/if}
  </div>

  <div style="column: 2; row: 1;">
    <div class="file-window">
      <h1>FILE MANAGMENT</h1>
      <form onsubmit={testme}>
        <!-- FIX THIS width -->
        <input id="test-input" placeholder="Tag Id" style="width: calc(100% - 3em);" bind:value={searchBar} />
        <button type="submit">Get files</button>
      </form>

      {#each files as file}
        <div class="file-entry" onclick={() => focusFile(file)}>{file.path}</div>
      {/each}
    </div>
  </div>
</main>

<style>

:root {
  font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
  font-size: 16px;
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
  display: grid;
  grid-template-columns: 1fr 3fr;
  grid-gap: 0.4vh;
  padding: 0.4vh;
}

.edit-tag-window {
  padding: 0.5vh;
}

.edit-file-window {
  padding: 0.5vh;
}

.tag-window {
  padding: 0.5vh;
}

.tag-field {
  padding: 0.5vh;
}

.tag {
  font-size: 3rem;
  border-radius: 0.6vh;
  padding: 0.2vh 1vh 0.2vh 1vh;
  margin: 0.3vh 0.3vh 0.3vh 0.3vh;
}

.edit-tag {
  font-size: 3rem;
  border-radius: 0.6vh 0 0 0.6vh;
  padding: 0.2vh 1vh 0.2vh 1vh;
  margin: 0.3vh 0 0.3vh 0.3vh;
}

.remove-tag {
  font-size: 3rem;
  border-radius: 0 0.6vh 0.6vh 0;
  padding: 0.2vh 0.8vh 0.2vh 0.5vh;
  margin: 0.3vh 0.3vh 0.3vh 0;
}

.file-window {
  padding: 0.5vh;
}

.file-entry {
  padding: 0.5vh;
  font-size: 4rem;
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

h1 {
  margin-bottom: 1.5vh;
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
