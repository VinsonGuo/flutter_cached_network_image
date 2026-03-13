import 'dart:async';
import 'dart:typed_data';
import 'dart:ui' as ui;
import 'dart:ui';

import 'package:cached_network_image_platform_interface/cached_network_image_platform_interface.dart';
import 'package:cached_network_image_platform_interface'
        '/cached_network_image_platform_interface.dart' as platform
    show ImageLoader;
import 'package:flutter/material.dart';
import 'package:flutter_cache_manager/flutter_cache_manager.dart';
import 'package:flutter_svg/flutter_svg.dart';

/// ImageLoader class to load images on IO platforms.
class ImageLoader implements platform.ImageLoader {
  @Deprecated('Use loadImageAsync instead')
  @override
  Stream<ui.Codec> loadBufferAsync(
    String url,
    String? cacheKey,
    StreamController<ImageChunkEvent> chunkEvents,
    DecoderBufferCallback decode,
    BaseCacheManager cacheManager,
    int? maxHeight,
    int? maxWidth,
    Map<String, String>? headers,
    ImageRenderMethodForWeb imageRenderMethodForWeb,
    VoidCallback evictImage,
  ) {
    return _load(
      url,
      cacheKey,
      chunkEvents,
      (bytes) async {
        final buffer = await ImmutableBuffer.fromUint8List(bytes);
        return decode(buffer);
      },
      cacheManager,
      maxHeight,
      maxWidth,
      headers,
      imageRenderMethodForWeb,
      evictImage,
    );
  }

  @override
  Stream<ui.Codec> loadImageAsync(
    String url,
    String? cacheKey,
    StreamController<ImageChunkEvent> chunkEvents,
    ImageDecoderCallback decode,
    BaseCacheManager cacheManager,
    int? maxHeight,
    int? maxWidth,
    Map<String, String>? headers,
    ImageRenderMethodForWeb imageRenderMethodForWeb,
    VoidCallback evictImage,
  ) {
    return _load(
      url,
      cacheKey,
      chunkEvents,
      (bytes) async {
        final buffer = await ImmutableBuffer.fromUint8List(bytes);
        return decode(buffer);
      },
      cacheManager,
      maxHeight,
      maxWidth,
      headers,
      imageRenderMethodForWeb,
      evictImage,
    );
  }

  Stream<ui.Codec> _load(
    String url,
    String? cacheKey,
    StreamController<ImageChunkEvent> chunkEvents,
    Future<ui.Codec> Function(Uint8List) decode,
    BaseCacheManager cacheManager,
    int? maxHeight,
    int? maxWidth,
    Map<String, String>? headers,
    ImageRenderMethodForWeb imageRenderMethodForWeb,
    VoidCallback evictImage,
  ) async* {
    try {
      assert(
          cacheManager is ImageCacheManager ||
              (maxWidth == null && maxHeight == null),
          'To resize the image with a CacheManager the '
          'CacheManager needs to be an ImageCacheManager. maxWidth and '
          'maxHeight will be ignored when a normal CacheManager is used.');

      final stream = cacheManager is ImageCacheManager
          ? cacheManager.getImageFile(
              url,
              maxHeight: maxHeight,
              maxWidth: maxWidth,
              withProgress: true,
              headers: headers,
              key: cacheKey,
            )
          : cacheManager.getFileStream(
              url,
              withProgress: true,
              headers: headers,
              key: cacheKey,
            );

      await for (final result in stream) {
        if (result is DownloadProgress) {
          chunkEvents.add(
            ImageChunkEvent(
              cumulativeBytesLoaded: result.downloaded,
              expectedTotalBytes: result.totalSize,
            ),
          );
        }
        if (result is FileInfo) {
          final file = result.file;
          final bytes = await file.readAsBytes();

          // Check if the file is an SVG
          if (_isSvg(file.path, bytes)) {
            final codec = await _decodeSvg(bytes);
            yield codec;
          } else {
            final decoded = await decode(bytes);
            yield decoded;
          }
        }
      }
    } on Object catch (error, stackTrace) {
      // Depending on where the exception was thrown, the image cache may not
      // have had a chance to track the key in the cache at all.
      // Schedule a microtask to give the cache a chance to add the key.
      scheduleMicrotask(() {
        evictImage();
      });
      yield* Stream.error(error, stackTrace);
    } finally {
      await chunkEvents.close();
    }
  }

  bool _isSvg(String filePath, Uint8List bytes) {
    // Check by file extension
    if (filePath.toLowerCase().endsWith('.svg')) {
      return true;
    }

    // Check by content (SVG files start with XML declaration or <svg)
    final content = String.fromCharCodes(bytes.take(20));
    final trimmed = content.trim();
    if (trimmed.startsWith('<?xml') ||
        trimmed.startsWith('<svg') ||
        trimmed.startsWith('<!DOCTYPE svg')) {
      return true;
    }

    return false;
  }

  Future<ui.Codec> _decodeSvg(Uint8List bytes,) async {
    // Use SvgBytesLoader for better encoding handling
    const scale = 3.0;
    final pictureInfo = await vg.loadPicture(
      SvgBytesLoader(bytes),
      null,
    );

    final picture = pictureInfo.picture;
    final size = pictureInfo.size;

    double targetWidth = size.width * scale;
    double targetHeight = size.height * scale;


    final recorder = ui.PictureRecorder();
    final canvas = ui.Canvas(recorder);

    canvas.scale(scale);
    canvas.drawPicture(picture);

    final image = await recorder.endRecording().toImage(
      targetWidth.ceil(),
      targetHeight.ceil(),
    );
    picture.dispose();

    // Create a single-frame codec from the image with scale info
    return _SingleFrameCodec(image);
  }
}

/// A simple single-frame codec for SVG images
class _SingleFrameCodec implements ui.Codec {
  _SingleFrameCodec(this._image);

  final ui.Image _image;

  @override
  int get frameCount => 1;

  @override
  int get repetitionCount => 0;

  @override
  Future<ui.FrameInfo> getNextFrame() async {
    return _SingleFrameInfo(_image.clone());
  }

  @override
  void dispose() {
    _image.dispose();
  }
}

class _SingleFrameInfo implements ui.FrameInfo {
  _SingleFrameInfo(this._image);

  final ui.Image _image;

  @override
  ui.Image get image => _image;

  @override
  Duration get duration => Duration.zero;
}
