// String-id convenience layer over the generated [tagnet.TagnetApp].
//
// The generated API is split: listings return DTOs with *string* ids
// (FileEntry.fileId, TagEntry.tagId), but the write methods take *opaque* id
// handles (FileId, TagId) that Dart can only obtain via resolveFileId /
// resolveTagId. This extension centralises the "resolve the string, then call
// the op" pattern so the UI can work purely in terms of the id strings it
// already has from the DTOs. (The query methods that *return* string ids —
// tagIdsForFileString / fileIdsForTagString — live on the generated API
// directly, since the bridge already returns strings there.)

import 'rust/api.dart' as tagnet;

extension TagnetServiceX on tagnet.TagnetApp {
  /// Delete a tag by its string id (as shown in TagEntry.tagId).
  Future<void> deleteTagByString(String tagId) async {
    await deleteTag(tagId: await resolveTagId(prefix: tagId));
  }

  /// Delete a file by its string id (as shown in FileEntry.fileId).
  Future<void> deleteFileByString(String fileId) async {
    await deleteFile(fileId: await resolveFileId(prefix: fileId));
  }

  /// Apply a tag (by string id) to a file (by string id).
  Future<void> tagFileByString({
    required String tagId,
    required String fileId,
  }) async {
    final tid = await resolveTagId(prefix: tagId);
    final fid = await resolveFileId(prefix: fileId);
    await tagFile(tagId: tid, fileId: fid);
  }

  /// Remove a tag (by string id) from a file (by string id).
  Future<void> untagFileByString({
    required String tagId,
    required String fileId,
  }) async {
    final tid = await resolveTagId(prefix: tagId);
    final fid = await resolveFileId(prefix: fileId);
    await untagFile(tagId: tid, fileId: fid);
  }
}
