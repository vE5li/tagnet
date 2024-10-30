<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";

  let name = $state("");
  let color = $state("#8f008f");

  let focusedTag = $state(null);
  let tags = $state([]);

  let tag = $state("");
  let items = $state([]);

  let editTagName = $state("");
  let editTagColor = $state("");
  let editTags = $state([]);
  let editSubtags = $state([]);
  let editAddingTag = $state(null);

  async function loadInitial() {
    tags = await invoke("all_tags", { });
  }

  setTimeout(loadInitial, 10);

  async function focusTag(tag) {
    // TODO: Make this an enum.
    if (editAddingTag == "tags") {
      await invoke("tag_tag", { tagId: tag.id, subtagId: focusedTag.id });
      editTags = await invoke("tags_for_subtag", { subtagId: focusedTag.id });
      editAddingTag = null;
      return;
    } else if (editAddingTag == "subtags") {
      await invoke("tag_tag", { tagId: focusedTag.id, subtagId: tag.id });
      editSubtags = await invoke("subtags_for_tag", { tagId: focusedTag.id });
      editAddingTag = null;
      return;
    }

    focusedTag = tag;
    editTagName = tag.name;
    editTagColor = tag.color;
    editSubtags = await invoke("subtags_for_tag", { tagId: tag.id });
    editTags = await invoke("tags_for_subtag", { subtagId: tag.id });
  }

  async function untagTag(tag) {
    await invoke("untag_tag", { tagId: tag.id, subtagId: focusedTag.id });
    editTags = await invoke("tags_for_subtag", { subtagId: focusedTag.id });
  }

  async function untagSubtag(tag) {
    await invoke("untag_tag", { tagId: focusedTag.id, subtagId: tag.id });
    editSubtags = await invoke("subtags_for_tag", { tagId: focusedTag.id });
  }

  async function addTag(event: Event) {
    event.preventDefault();
    await invoke("add_tag", { name, color });
    tags = await invoke("all_tags", { });
    name = "";
  }

  async function updateTag(event: Event) {
    event.preventDefault();
    await invoke("update_tag", { tagId: focusedTag.id, name: editTagName, color: editTagColor });
    // TODO: Don't fetch all tags
    tags = await invoke("all_tags", { });
  }

  async function testme(event: Event) {
    event.preventDefault();
    items = await invoke("files_for_tag", { tag });
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
          <button class="tag" style="background: {tag.color}" onclick={function() { focusTag(tag) }}>{tag.name}</button>
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
        {#each editTags as tag}
          <button class="edit-tag" style="background: {tag.color}" onclick={function() { focusTag(tag) }}>{tag.name}</button>
          <button class="remove-tag" style="background: #888888" onclick={function() { untagTag(tag) }}>-</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={editAddingTag = "tags"}>+</button>
        <h1>SUBTAGS</h1>
        {#each editSubtags as tag}
          <button class="edit-tag" style="background: {tag.color}" onclick={function() { focusTag(tag) }}>{tag.name}</button>
          <button class="remove-tag" style="background: #888888" onclick={function() { untagSubtag(tag) }}>-</button>
        {/each}
        <button class="tag" style="background-color: #888888" onclick={editAddingTag = "subtags"}>+</button>
      </div>
      {#if editAddingTag}
        <h1>Adding {editAddingTag}</h1>
        <button class="tag" style="background-color: #888888" onclick={editAddingTag = null}>Cancel</button>
      {/if}
    {/if}
  </div>

  <div style="column: 2; row: 1;">
    <div class="file-window">
      <h1>FILE MANAGMENT</h1>
      <form onsubmit={testme}>
        <!-- FIX THIS width -->
        <input id="test-input" placeholder="Tag Id" style="width: calc(100% - 3em);" bind:value={tag} />
        <button type="submit">Get files</button>
      </form>

      {#each items as item}
        <h1>{item}</h1>
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
