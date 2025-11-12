var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// node_modules/fflate/esm/browser.js
var browser_exports = {};
__export(browser_exports, {
  AsyncCompress: () => AsyncGzip,
  AsyncDecompress: () => AsyncDecompress,
  AsyncDeflate: () => AsyncDeflate,
  AsyncGunzip: () => AsyncGunzip,
  AsyncGzip: () => AsyncGzip,
  AsyncInflate: () => AsyncInflate,
  AsyncUnzipInflate: () => AsyncUnzipInflate,
  AsyncUnzlib: () => AsyncUnzlib,
  AsyncZipDeflate: () => AsyncZipDeflate,
  AsyncZlib: () => AsyncZlib,
  Compress: () => Gzip,
  DecodeUTF8: () => DecodeUTF8,
  Decompress: () => Decompress,
  Deflate: () => Deflate,
  EncodeUTF8: () => EncodeUTF8,
  FlateErrorCode: () => FlateErrorCode,
  Gunzip: () => Gunzip,
  Gzip: () => Gzip,
  Inflate: () => Inflate,
  Unzip: () => Unzip,
  UnzipInflate: () => UnzipInflate,
  UnzipPassThrough: () => UnzipPassThrough,
  Unzlib: () => Unzlib,
  Zip: () => Zip,
  ZipDeflate: () => ZipDeflate,
  ZipPassThrough: () => ZipPassThrough,
  Zlib: () => Zlib,
  compress: () => gzip,
  compressSync: () => gzipSync,
  decompress: () => decompress,
  decompressSync: () => decompressSync,
  deflate: () => deflate,
  deflateSync: () => deflateSync,
  gunzip: () => gunzip,
  gunzipSync: () => gunzipSync,
  gzip: () => gzip,
  gzipSync: () => gzipSync,
  inflate: () => inflate,
  inflateSync: () => inflateSync,
  strFromU8: () => strFromU8,
  strToU8: () => strToU8,
  unzip: () => unzip,
  unzipSync: () => unzipSync,
  unzlib: () => unzlib,
  unzlibSync: () => unzlibSync,
  zip: () => zip,
  zipSync: () => zipSync,
  zlib: () => zlib,
  zlibSync: () => zlibSync
});
function StrmOpt(opts, cb) {
  if (typeof opts == "function")
    cb = opts, opts = {};
  this.ondata = cb;
  return opts;
}
function deflate(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bDflt
  ], function(ev) {
    return pbf(deflateSync(ev.data[0], ev.data[1]));
  }, 0, cb);
}
function deflateSync(data, opts) {
  return dopt(data, opts || {}, 0, 0);
}
function inflate(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bInflt
  ], function(ev) {
    return pbf(inflateSync(ev.data[0], gopt(ev.data[1])));
  }, 1, cb);
}
function inflateSync(data, opts) {
  return inflt(data, { i: 2 }, opts && opts.out, opts && opts.dictionary);
}
function gzip(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bDflt,
    gze,
    function() {
      return [gzipSync];
    }
  ], function(ev) {
    return pbf(gzipSync(ev.data[0], ev.data[1]));
  }, 2, cb);
}
function gzipSync(data, opts) {
  if (!opts)
    opts = {};
  var c = crc(), l = data.length;
  c.p(data);
  var d = dopt(data, opts, gzhl(opts), 8), s = d.length;
  return gzh(d, opts), wbytes(d, s - 8, c.d()), wbytes(d, s - 4, l), d;
}
function gunzip(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bInflt,
    guze,
    function() {
      return [gunzipSync];
    }
  ], function(ev) {
    return pbf(gunzipSync(ev.data[0], ev.data[1]));
  }, 3, cb);
}
function gunzipSync(data, opts) {
  var st = gzs(data);
  if (st + 8 > data.length)
    err(6, "invalid gzip data");
  return inflt(data.subarray(st, -8), { i: 2 }, opts && opts.out || new u8(gzl(data)), opts && opts.dictionary);
}
function zlib(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bDflt,
    zle,
    function() {
      return [zlibSync];
    }
  ], function(ev) {
    return pbf(zlibSync(ev.data[0], ev.data[1]));
  }, 4, cb);
}
function zlibSync(data, opts) {
  if (!opts)
    opts = {};
  var a = adler();
  a.p(data);
  var d = dopt(data, opts, opts.dictionary ? 6 : 2, 4);
  return zlh(d, opts), wbytes(d, d.length - 4, a.d()), d;
}
function unzlib(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return cbify(data, opts, [
    bInflt,
    zule,
    function() {
      return [unzlibSync];
    }
  ], function(ev) {
    return pbf(unzlibSync(ev.data[0], gopt(ev.data[1])));
  }, 5, cb);
}
function unzlibSync(data, opts) {
  return inflt(data.subarray(zls(data, opts && opts.dictionary), -4), { i: 2 }, opts && opts.out, opts && opts.dictionary);
}
function decompress(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  return data[0] == 31 && data[1] == 139 && data[2] == 8 ? gunzip(data, opts, cb) : (data[0] & 15) != 8 || data[0] >> 4 > 7 || (data[0] << 8 | data[1]) % 31 ? inflate(data, opts, cb) : unzlib(data, opts, cb);
}
function decompressSync(data, opts) {
  return data[0] == 31 && data[1] == 139 && data[2] == 8 ? gunzipSync(data, opts) : (data[0] & 15) != 8 || data[0] >> 4 > 7 || (data[0] << 8 | data[1]) % 31 ? inflateSync(data, opts) : unzlibSync(data, opts);
}
function strToU8(str, latin1) {
  if (latin1) {
    var ar_1 = new u8(str.length);
    for (var i = 0; i < str.length; ++i)
      ar_1[i] = str.charCodeAt(i);
    return ar_1;
  }
  if (te)
    return te.encode(str);
  var l = str.length;
  var ar = new u8(str.length + (str.length >> 1));
  var ai = 0;
  var w = function(v) {
    ar[ai++] = v;
  };
  for (var i = 0; i < l; ++i) {
    if (ai + 5 > ar.length) {
      var n = new u8(ai + 8 + (l - i << 1));
      n.set(ar);
      ar = n;
    }
    var c = str.charCodeAt(i);
    if (c < 128 || latin1)
      w(c);
    else if (c < 2048)
      w(192 | c >> 6), w(128 | c & 63);
    else if (c > 55295 && c < 57344)
      c = 65536 + (c & 1023 << 10) | str.charCodeAt(++i) & 1023, w(240 | c >> 18), w(128 | c >> 12 & 63), w(128 | c >> 6 & 63), w(128 | c & 63);
    else
      w(224 | c >> 12), w(128 | c >> 6 & 63), w(128 | c & 63);
  }
  return slc(ar, 0, ai);
}
function strFromU8(dat, latin1) {
  if (latin1) {
    var r = "";
    for (var i = 0; i < dat.length; i += 16384)
      r += String.fromCharCode.apply(null, dat.subarray(i, i + 16384));
    return r;
  } else if (td) {
    return td.decode(dat);
  } else {
    var _a2 = dutf8(dat), s = _a2.s, r = _a2.r;
    if (r.length)
      err(8);
    return s;
  }
}
function zip(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  var r = {};
  fltn(data, "", r, opts);
  var k = Object.keys(r);
  var lft = k.length, o = 0, tot = 0;
  var slft = lft, files = new Array(lft);
  var term = [];
  var tAll = function() {
    for (var i2 = 0; i2 < term.length; ++i2)
      term[i2]();
  };
  var cbd = function(a, b) {
    mt(function() {
      cb(a, b);
    });
  };
  mt(function() {
    cbd = cb;
  });
  var cbf = function() {
    var out = new u8(tot + 22), oe = o, cdl = tot - o;
    tot = 0;
    for (var i2 = 0; i2 < slft; ++i2) {
      var f = files[i2];
      try {
        var l = f.c.length;
        wzh(out, tot, f, f.f, f.u, l);
        var badd = 30 + f.f.length + exfl(f.extra);
        var loc = tot + badd;
        out.set(f.c, loc);
        wzh(out, o, f, f.f, f.u, l, tot, f.m), o += 16 + badd + (f.m ? f.m.length : 0), tot = loc + l;
      } catch (e) {
        return cbd(e, null);
      }
    }
    wzf(out, o, files.length, cdl, oe);
    cbd(null, out);
  };
  if (!lft)
    cbf();
  var _loop_1 = function(i2) {
    var fn = k[i2];
    var _a2 = r[fn], file = _a2[0], p = _a2[1];
    var c = crc(), size = file.length;
    c.p(file);
    var f = strToU8(fn), s = f.length;
    var com = p.comment, m = com && strToU8(com), ms = m && m.length;
    var exl = exfl(p.extra);
    var compression = p.level == 0 ? 0 : 8;
    var cbl = function(e, d) {
      if (e) {
        tAll();
        cbd(e, null);
      } else {
        var l = d.length;
        files[i2] = mrg(p, {
          size,
          crc: c.d(),
          c: d,
          f,
          m,
          u: s != fn.length || m && com.length != ms,
          compression
        });
        o += 30 + s + exl + l;
        tot += 76 + 2 * (s + exl) + (ms || 0) + l;
        if (!--lft)
          cbf();
      }
    };
    if (s > 65535)
      cbl(err(11, 0, 1), null);
    if (!compression)
      cbl(null, file);
    else if (size < 16e4) {
      try {
        cbl(null, deflateSync(file, p));
      } catch (e) {
        cbl(e, null);
      }
    } else
      term.push(deflate(file, p, cbl));
  };
  for (var i = 0; i < slft; ++i) {
    _loop_1(i);
  }
  return tAll;
}
function zipSync(data, opts) {
  if (!opts)
    opts = {};
  var r = {};
  var files = [];
  fltn(data, "", r, opts);
  var o = 0;
  var tot = 0;
  for (var fn in r) {
    var _a2 = r[fn], file = _a2[0], p = _a2[1];
    var compression = p.level == 0 ? 0 : 8;
    var f = strToU8(fn), s = f.length;
    var com = p.comment, m = com && strToU8(com), ms = m && m.length;
    var exl = exfl(p.extra);
    if (s > 65535)
      err(11);
    var d = compression ? deflateSync(file, p) : file, l = d.length;
    var c = crc();
    c.p(file);
    files.push(mrg(p, {
      size: file.length,
      crc: c.d(),
      c: d,
      f,
      m,
      u: s != fn.length || m && com.length != ms,
      o,
      compression
    }));
    o += 30 + s + exl + l;
    tot += 76 + 2 * (s + exl) + (ms || 0) + l;
  }
  var out = new u8(tot + 22), oe = o, cdl = tot - o;
  for (var i = 0; i < files.length; ++i) {
    var f = files[i];
    wzh(out, f.o, f, f.f, f.u, f.c.length);
    var badd = 30 + f.f.length + exfl(f.extra);
    out.set(f.c, f.o + badd);
    wzh(out, o, f, f.f, f.u, f.c.length, f.o, f.m), o += 16 + badd + (f.m ? f.m.length : 0);
  }
  wzf(out, o, files.length, cdl, oe);
  return out;
}
function unzip(data, opts, cb) {
  if (!cb)
    cb = opts, opts = {};
  if (typeof cb != "function")
    err(7);
  var term = [];
  var tAll = function() {
    for (var i2 = 0; i2 < term.length; ++i2)
      term[i2]();
  };
  var files = {};
  var cbd = function(a, b) {
    mt(function() {
      cb(a, b);
    });
  };
  mt(function() {
    cbd = cb;
  });
  var e = data.length - 22;
  for (; b4(data, e) != 101010256; --e) {
    if (!e || data.length - e > 65558) {
      cbd(err(13, 0, 1), null);
      return tAll;
    }
  }
  ;
  var lft = b2(data, e + 8);
  if (lft) {
    var c = lft;
    var o = b4(data, e + 16);
    var z = o == 4294967295 || c == 65535;
    if (z) {
      var ze = b4(data, e - 12);
      z = b4(data, ze) == 101075792;
      if (z) {
        c = lft = b4(data, ze + 32);
        o = b4(data, ze + 48);
      }
    }
    var fltr = opts && opts.filter;
    var _loop_3 = function(i2) {
      var _a2 = zh(data, o, z), c_1 = _a2[0], sc = _a2[1], su = _a2[2], fn = _a2[3], no = _a2[4], off = _a2[5], b = slzh(data, off);
      o = no;
      var cbl = function(e2, d) {
        if (e2) {
          tAll();
          cbd(e2, null);
        } else {
          if (d)
            files[fn] = d;
          if (!--lft)
            cbd(null, files);
        }
      };
      if (!fltr || fltr({
        name: fn,
        size: sc,
        originalSize: su,
        compression: c_1
      })) {
        if (!c_1)
          cbl(null, slc(data, b, b + sc));
        else if (c_1 == 8) {
          var infl = data.subarray(b, b + sc);
          if (su < 524288 || sc > 0.8 * su) {
            try {
              cbl(null, inflateSync(infl, { out: new u8(su) }));
            } catch (e2) {
              cbl(e2, null);
            }
          } else
            term.push(inflate(infl, { size: su }, cbl));
        } else
          cbl(err(14, "unknown compression type " + c_1, 1), null);
      } else
        cbl(null, null);
    };
    for (var i = 0; i < c; ++i) {
      _loop_3(i);
    }
  } else
    cbd(null, {});
  return tAll;
}
function unzipSync(data, opts) {
  var files = {};
  var e = data.length - 22;
  for (; b4(data, e) != 101010256; --e) {
    if (!e || data.length - e > 65558)
      err(13);
  }
  ;
  var c = b2(data, e + 8);
  if (!c)
    return {};
  var o = b4(data, e + 16);
  var z = o == 4294967295 || c == 65535;
  if (z) {
    var ze = b4(data, e - 12);
    z = b4(data, ze) == 101075792;
    if (z) {
      c = b4(data, ze + 32);
      o = b4(data, ze + 48);
    }
  }
  var fltr = opts && opts.filter;
  for (var i = 0; i < c; ++i) {
    var _a2 = zh(data, o, z), c_2 = _a2[0], sc = _a2[1], su = _a2[2], fn = _a2[3], no = _a2[4], off = _a2[5], b = slzh(data, off);
    o = no;
    if (!fltr || fltr({
      name: fn,
      size: sc,
      originalSize: su,
      compression: c_2
    })) {
      if (!c_2)
        files[fn] = slc(data, b, b + sc);
      else if (c_2 == 8)
        files[fn] = inflateSync(data.subarray(b, b + sc), { out: new u8(su) });
      else
        err(14, "unknown compression type " + c_2);
    }
  }
  return files;
}
var ch2, wk, u8, u16, i32, fleb, fdeb, clim, freb, _a, fl, revfl, _b, fd, revfd, rev, x, i, hMap, flt, i, i, i, i, fdt, i, flm, flrm, fdm, fdrm, max, bits, bits16, shft, slc, FlateErrorCode, ec, err, inflt, wbits, wbits16, hTree, ln, lc, clen, wfblk, wblk, deo, et, dflt, crct, crc, adler, dopt, mrg, wcln, ch, cbfs, wrkr, bInflt, bDflt, gze, guze, zle, zule, pbf, gopt, cbify, astrm, astrmify, b2, b4, b8, wbytes, gzh, gzs, gzl, gzhl, zlh, zls, Deflate, AsyncDeflate, Inflate, AsyncInflate, Gzip, AsyncGzip, Gunzip, AsyncGunzip, Zlib, AsyncZlib, Unzlib, AsyncUnzlib, Decompress, AsyncDecompress, fltn, te, td, tds, dutf8, DecodeUTF8, EncodeUTF8, dbf, slzh, zh, z64e, exfl, wzh, wzf, ZipPassThrough, ZipDeflate, AsyncZipDeflate, Zip, UnzipPassThrough, UnzipInflate, AsyncUnzipInflate, Unzip, mt;
var init_browser = __esm({
  "node_modules/fflate/esm/browser.js"() {
    ch2 = {};
    wk = function(c, id, msg, transfer, cb) {
      var w = new Worker(ch2[id] || (ch2[id] = URL.createObjectURL(new Blob([
        c + ';addEventListener("error",function(e){e=e.error;postMessage({$e$:[e.message,e.code,e.stack]})})'
      ], { type: "text/javascript" }))));
      w.onmessage = function(e) {
        var d = e.data, ed = d.$e$;
        if (ed) {
          var err2 = new Error(ed[0]);
          err2["code"] = ed[1];
          err2.stack = ed[2];
          cb(err2, null);
        } else
          cb(null, d);
      };
      w.postMessage(msg, transfer);
      return w;
    };
    u8 = Uint8Array;
    u16 = Uint16Array;
    i32 = Int32Array;
    fleb = new u8([
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      1,
      1,
      1,
      2,
      2,
      2,
      2,
      3,
      3,
      3,
      3,
      4,
      4,
      4,
      4,
      5,
      5,
      5,
      5,
      0,
      /* unused */
      0,
      0,
      /* impossible */
      0
    ]);
    fdeb = new u8([
      0,
      0,
      0,
      0,
      1,
      1,
      2,
      2,
      3,
      3,
      4,
      4,
      5,
      5,
      6,
      6,
      7,
      7,
      8,
      8,
      9,
      9,
      10,
      10,
      11,
      11,
      12,
      12,
      13,
      13,
      /* unused */
      0,
      0
    ]);
    clim = new u8([16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15]);
    freb = function(eb, start) {
      var b = new u16(31);
      for (var i = 0; i < 31; ++i) {
        b[i] = start += 1 << eb[i - 1];
      }
      var r = new i32(b[30]);
      for (var i = 1; i < 30; ++i) {
        for (var j = b[i]; j < b[i + 1]; ++j) {
          r[j] = j - b[i] << 5 | i;
        }
      }
      return { b, r };
    };
    _a = freb(fleb, 2);
    fl = _a.b;
    revfl = _a.r;
    fl[28] = 258, revfl[258] = 28;
    _b = freb(fdeb, 0);
    fd = _b.b;
    revfd = _b.r;
    rev = new u16(32768);
    for (i = 0; i < 32768; ++i) {
      x = (i & 43690) >> 1 | (i & 21845) << 1;
      x = (x & 52428) >> 2 | (x & 13107) << 2;
      x = (x & 61680) >> 4 | (x & 3855) << 4;
      rev[i] = ((x & 65280) >> 8 | (x & 255) << 8) >> 1;
    }
    hMap = function(cd, mb, r) {
      var s = cd.length;
      var i = 0;
      var l = new u16(mb);
      for (; i < s; ++i) {
        if (cd[i])
          ++l[cd[i] - 1];
      }
      var le = new u16(mb);
      for (i = 1; i < mb; ++i) {
        le[i] = le[i - 1] + l[i - 1] << 1;
      }
      var co;
      if (r) {
        co = new u16(1 << mb);
        var rvb = 15 - mb;
        for (i = 0; i < s; ++i) {
          if (cd[i]) {
            var sv = i << 4 | cd[i];
            var r_1 = mb - cd[i];
            var v = le[cd[i] - 1]++ << r_1;
            for (var m = v | (1 << r_1) - 1; v <= m; ++v) {
              co[rev[v] >> rvb] = sv;
            }
          }
        }
      } else {
        co = new u16(s);
        for (i = 0; i < s; ++i) {
          if (cd[i]) {
            co[i] = rev[le[cd[i] - 1]++] >> 15 - cd[i];
          }
        }
      }
      return co;
    };
    flt = new u8(288);
    for (i = 0; i < 144; ++i)
      flt[i] = 8;
    for (i = 144; i < 256; ++i)
      flt[i] = 9;
    for (i = 256; i < 280; ++i)
      flt[i] = 7;
    for (i = 280; i < 288; ++i)
      flt[i] = 8;
    fdt = new u8(32);
    for (i = 0; i < 32; ++i)
      fdt[i] = 5;
    flm = /* @__PURE__ */ hMap(flt, 9, 0);
    flrm = /* @__PURE__ */ hMap(flt, 9, 1);
    fdm = /* @__PURE__ */ hMap(fdt, 5, 0);
    fdrm = /* @__PURE__ */ hMap(fdt, 5, 1);
    max = function(a) {
      var m = a[0];
      for (var i = 1; i < a.length; ++i) {
        if (a[i] > m)
          m = a[i];
      }
      return m;
    };
    bits = function(d, p, m) {
      var o = p / 8 | 0;
      return (d[o] | d[o + 1] << 8) >> (p & 7) & m;
    };
    bits16 = function(d, p) {
      var o = p / 8 | 0;
      return (d[o] | d[o + 1] << 8 | d[o + 2] << 16) >> (p & 7);
    };
    shft = function(p) {
      return (p + 7) / 8 | 0;
    };
    slc = function(v, s, e) {
      if (s == null || s < 0)
        s = 0;
      if (e == null || e > v.length)
        e = v.length;
      return new u8(v.subarray(s, e));
    };
    FlateErrorCode = {
      UnexpectedEOF: 0,
      InvalidBlockType: 1,
      InvalidLengthLiteral: 2,
      InvalidDistance: 3,
      StreamFinished: 4,
      NoStreamHandler: 5,
      InvalidHeader: 6,
      NoCallback: 7,
      InvalidUTF8: 8,
      ExtraFieldTooLong: 9,
      InvalidDate: 10,
      FilenameTooLong: 11,
      StreamFinishing: 12,
      InvalidZipData: 13,
      UnknownCompressionMethod: 14
    };
    ec = [
      "unexpected EOF",
      "invalid block type",
      "invalid length/literal",
      "invalid distance",
      "stream finished",
      "no stream handler",
      ,
      "no callback",
      "invalid UTF-8 data",
      "extra field too long",
      "date not in range 1980-2099",
      "filename too long",
      "stream finishing",
      "invalid zip data"
      // determined by unknown compression method
    ];
    err = function(ind, msg, nt) {
      var e = new Error(msg || ec[ind]);
      e.code = ind;
      if (Error.captureStackTrace)
        Error.captureStackTrace(e, err);
      if (!nt)
        throw e;
      return e;
    };
    inflt = function(dat, st, buf, dict) {
      var sl = dat.length, dl = dict ? dict.length : 0;
      if (!sl || st.f && !st.l)
        return buf || new u8(0);
      var noBuf = !buf;
      var resize = noBuf || st.i != 2;
      var noSt = st.i;
      if (noBuf)
        buf = new u8(sl * 3);
      var cbuf = function(l2) {
        var bl = buf.length;
        if (l2 > bl) {
          var nbuf = new u8(Math.max(bl * 2, l2));
          nbuf.set(buf);
          buf = nbuf;
        }
      };
      var final = st.f || 0, pos = st.p || 0, bt = st.b || 0, lm = st.l, dm = st.d, lbt = st.m, dbt = st.n;
      var tbts = sl * 8;
      do {
        if (!lm) {
          final = bits(dat, pos, 1);
          var type = bits(dat, pos + 1, 3);
          pos += 3;
          if (!type) {
            var s = shft(pos) + 4, l = dat[s - 4] | dat[s - 3] << 8, t = s + l;
            if (t > sl) {
              if (noSt)
                err(0);
              break;
            }
            if (resize)
              cbuf(bt + l);
            buf.set(dat.subarray(s, t), bt);
            st.b = bt += l, st.p = pos = t * 8, st.f = final;
            continue;
          } else if (type == 1)
            lm = flrm, dm = fdrm, lbt = 9, dbt = 5;
          else if (type == 2) {
            var hLit = bits(dat, pos, 31) + 257, hcLen = bits(dat, pos + 10, 15) + 4;
            var tl = hLit + bits(dat, pos + 5, 31) + 1;
            pos += 14;
            var ldt = new u8(tl);
            var clt = new u8(19);
            for (var i = 0; i < hcLen; ++i) {
              clt[clim[i]] = bits(dat, pos + i * 3, 7);
            }
            pos += hcLen * 3;
            var clb = max(clt), clbmsk = (1 << clb) - 1;
            var clm = hMap(clt, clb, 1);
            for (var i = 0; i < tl; ) {
              var r = clm[bits(dat, pos, clbmsk)];
              pos += r & 15;
              var s = r >> 4;
              if (s < 16) {
                ldt[i++] = s;
              } else {
                var c = 0, n = 0;
                if (s == 16)
                  n = 3 + bits(dat, pos, 3), pos += 2, c = ldt[i - 1];
                else if (s == 17)
                  n = 3 + bits(dat, pos, 7), pos += 3;
                else if (s == 18)
                  n = 11 + bits(dat, pos, 127), pos += 7;
                while (n--)
                  ldt[i++] = c;
              }
            }
            var lt = ldt.subarray(0, hLit), dt = ldt.subarray(hLit);
            lbt = max(lt);
            dbt = max(dt);
            lm = hMap(lt, lbt, 1);
            dm = hMap(dt, dbt, 1);
          } else
            err(1);
          if (pos > tbts) {
            if (noSt)
              err(0);
            break;
          }
        }
        if (resize)
          cbuf(bt + 131072);
        var lms = (1 << lbt) - 1, dms = (1 << dbt) - 1;
        var lpos = pos;
        for (; ; lpos = pos) {
          var c = lm[bits16(dat, pos) & lms], sym = c >> 4;
          pos += c & 15;
          if (pos > tbts) {
            if (noSt)
              err(0);
            break;
          }
          if (!c)
            err(2);
          if (sym < 256)
            buf[bt++] = sym;
          else if (sym == 256) {
            lpos = pos, lm = null;
            break;
          } else {
            var add = sym - 254;
            if (sym > 264) {
              var i = sym - 257, b = fleb[i];
              add = bits(dat, pos, (1 << b) - 1) + fl[i];
              pos += b;
            }
            var d = dm[bits16(dat, pos) & dms], dsym = d >> 4;
            if (!d)
              err(3);
            pos += d & 15;
            var dt = fd[dsym];
            if (dsym > 3) {
              var b = fdeb[dsym];
              dt += bits16(dat, pos) & (1 << b) - 1, pos += b;
            }
            if (pos > tbts) {
              if (noSt)
                err(0);
              break;
            }
            if (resize)
              cbuf(bt + 131072);
            var end = bt + add;
            if (bt < dt) {
              var shift = dl - dt, dend = Math.min(dt, end);
              if (shift + bt < 0)
                err(3);
              for (; bt < dend; ++bt)
                buf[bt] = dict[shift + bt];
            }
            for (; bt < end; ++bt)
              buf[bt] = buf[bt - dt];
          }
        }
        st.l = lm, st.p = lpos, st.b = bt, st.f = final;
        if (lm)
          final = 1, st.m = lbt, st.d = dm, st.n = dbt;
      } while (!final);
      return bt != buf.length && noBuf ? slc(buf, 0, bt) : buf.subarray(0, bt);
    };
    wbits = function(d, p, v) {
      v <<= p & 7;
      var o = p / 8 | 0;
      d[o] |= v;
      d[o + 1] |= v >> 8;
    };
    wbits16 = function(d, p, v) {
      v <<= p & 7;
      var o = p / 8 | 0;
      d[o] |= v;
      d[o + 1] |= v >> 8;
      d[o + 2] |= v >> 16;
    };
    hTree = function(d, mb) {
      var t = [];
      for (var i = 0; i < d.length; ++i) {
        if (d[i])
          t.push({ s: i, f: d[i] });
      }
      var s = t.length;
      var t2 = t.slice();
      if (!s)
        return { t: et, l: 0 };
      if (s == 1) {
        var v = new u8(t[0].s + 1);
        v[t[0].s] = 1;
        return { t: v, l: 1 };
      }
      t.sort(function(a, b) {
        return a.f - b.f;
      });
      t.push({ s: -1, f: 25001 });
      var l = t[0], r = t[1], i0 = 0, i1 = 1, i2 = 2;
      t[0] = { s: -1, f: l.f + r.f, l, r };
      while (i1 != s - 1) {
        l = t[t[i0].f < t[i2].f ? i0++ : i2++];
        r = t[i0 != i1 && t[i0].f < t[i2].f ? i0++ : i2++];
        t[i1++] = { s: -1, f: l.f + r.f, l, r };
      }
      var maxSym = t2[0].s;
      for (var i = 1; i < s; ++i) {
        if (t2[i].s > maxSym)
          maxSym = t2[i].s;
      }
      var tr = new u16(maxSym + 1);
      var mbt = ln(t[i1 - 1], tr, 0);
      if (mbt > mb) {
        var i = 0, dt = 0;
        var lft = mbt - mb, cst = 1 << lft;
        t2.sort(function(a, b) {
          return tr[b.s] - tr[a.s] || a.f - b.f;
        });
        for (; i < s; ++i) {
          var i2_1 = t2[i].s;
          if (tr[i2_1] > mb) {
            dt += cst - (1 << mbt - tr[i2_1]);
            tr[i2_1] = mb;
          } else
            break;
        }
        dt >>= lft;
        while (dt > 0) {
          var i2_2 = t2[i].s;
          if (tr[i2_2] < mb)
            dt -= 1 << mb - tr[i2_2]++ - 1;
          else
            ++i;
        }
        for (; i >= 0 && dt; --i) {
          var i2_3 = t2[i].s;
          if (tr[i2_3] == mb) {
            --tr[i2_3];
            ++dt;
          }
        }
        mbt = mb;
      }
      return { t: new u8(tr), l: mbt };
    };
    ln = function(n, l, d) {
      return n.s == -1 ? Math.max(ln(n.l, l, d + 1), ln(n.r, l, d + 1)) : l[n.s] = d;
    };
    lc = function(c) {
      var s = c.length;
      while (s && !c[--s])
        ;
      var cl = new u16(++s);
      var cli = 0, cln = c[0], cls = 1;
      var w = function(v) {
        cl[cli++] = v;
      };
      for (var i = 1; i <= s; ++i) {
        if (c[i] == cln && i != s)
          ++cls;
        else {
          if (!cln && cls > 2) {
            for (; cls > 138; cls -= 138)
              w(32754);
            if (cls > 2) {
              w(cls > 10 ? cls - 11 << 5 | 28690 : cls - 3 << 5 | 12305);
              cls = 0;
            }
          } else if (cls > 3) {
            w(cln), --cls;
            for (; cls > 6; cls -= 6)
              w(8304);
            if (cls > 2)
              w(cls - 3 << 5 | 8208), cls = 0;
          }
          while (cls--)
            w(cln);
          cls = 1;
          cln = c[i];
        }
      }
      return { c: cl.subarray(0, cli), n: s };
    };
    clen = function(cf, cl) {
      var l = 0;
      for (var i = 0; i < cl.length; ++i)
        l += cf[i] * cl[i];
      return l;
    };
    wfblk = function(out, pos, dat) {
      var s = dat.length;
      var o = shft(pos + 2);
      out[o] = s & 255;
      out[o + 1] = s >> 8;
      out[o + 2] = out[o] ^ 255;
      out[o + 3] = out[o + 1] ^ 255;
      for (var i = 0; i < s; ++i)
        out[o + i + 4] = dat[i];
      return (o + 4 + s) * 8;
    };
    wblk = function(dat, out, final, syms, lf, df, eb, li, bs, bl, p) {
      wbits(out, p++, final);
      ++lf[256];
      var _a2 = hTree(lf, 15), dlt = _a2.t, mlb = _a2.l;
      var _b2 = hTree(df, 15), ddt = _b2.t, mdb = _b2.l;
      var _c = lc(dlt), lclt = _c.c, nlc = _c.n;
      var _d = lc(ddt), lcdt = _d.c, ndc = _d.n;
      var lcfreq = new u16(19);
      for (var i = 0; i < lclt.length; ++i)
        ++lcfreq[lclt[i] & 31];
      for (var i = 0; i < lcdt.length; ++i)
        ++lcfreq[lcdt[i] & 31];
      var _e = hTree(lcfreq, 7), lct = _e.t, mlcb = _e.l;
      var nlcc = 19;
      for (; nlcc > 4 && !lct[clim[nlcc - 1]]; --nlcc)
        ;
      var flen = bl + 5 << 3;
      var ftlen = clen(lf, flt) + clen(df, fdt) + eb;
      var dtlen = clen(lf, dlt) + clen(df, ddt) + eb + 14 + 3 * nlcc + clen(lcfreq, lct) + 2 * lcfreq[16] + 3 * lcfreq[17] + 7 * lcfreq[18];
      if (bs >= 0 && flen <= ftlen && flen <= dtlen)
        return wfblk(out, p, dat.subarray(bs, bs + bl));
      var lm, ll, dm, dl;
      wbits(out, p, 1 + (dtlen < ftlen)), p += 2;
      if (dtlen < ftlen) {
        lm = hMap(dlt, mlb, 0), ll = dlt, dm = hMap(ddt, mdb, 0), dl = ddt;
        var llm = hMap(lct, mlcb, 0);
        wbits(out, p, nlc - 257);
        wbits(out, p + 5, ndc - 1);
        wbits(out, p + 10, nlcc - 4);
        p += 14;
        for (var i = 0; i < nlcc; ++i)
          wbits(out, p + 3 * i, lct[clim[i]]);
        p += 3 * nlcc;
        var lcts = [lclt, lcdt];
        for (var it = 0; it < 2; ++it) {
          var clct = lcts[it];
          for (var i = 0; i < clct.length; ++i) {
            var len = clct[i] & 31;
            wbits(out, p, llm[len]), p += lct[len];
            if (len > 15)
              wbits(out, p, clct[i] >> 5 & 127), p += clct[i] >> 12;
          }
        }
      } else {
        lm = flm, ll = flt, dm = fdm, dl = fdt;
      }
      for (var i = 0; i < li; ++i) {
        var sym = syms[i];
        if (sym > 255) {
          var len = sym >> 18 & 31;
          wbits16(out, p, lm[len + 257]), p += ll[len + 257];
          if (len > 7)
            wbits(out, p, sym >> 23 & 31), p += fleb[len];
          var dst = sym & 31;
          wbits16(out, p, dm[dst]), p += dl[dst];
          if (dst > 3)
            wbits16(out, p, sym >> 5 & 8191), p += fdeb[dst];
        } else {
          wbits16(out, p, lm[sym]), p += ll[sym];
        }
      }
      wbits16(out, p, lm[256]);
      return p + ll[256];
    };
    deo = /* @__PURE__ */ new i32([65540, 131080, 131088, 131104, 262176, 1048704, 1048832, 2114560, 2117632]);
    et = /* @__PURE__ */ new u8(0);
    dflt = function(dat, lvl, plvl, pre, post, st) {
      var s = st.z || dat.length;
      var o = new u8(pre + s + 5 * (1 + Math.ceil(s / 7e3)) + post);
      var w = o.subarray(pre, o.length - post);
      var lst = st.l;
      var pos = (st.r || 0) & 7;
      if (lvl) {
        if (pos)
          w[0] = st.r >> 3;
        var opt = deo[lvl - 1];
        var n = opt >> 13, c = opt & 8191;
        var msk_1 = (1 << plvl) - 1;
        var prev = st.p || new u16(32768), head = st.h || new u16(msk_1 + 1);
        var bs1_1 = Math.ceil(plvl / 3), bs2_1 = 2 * bs1_1;
        var hsh = function(i2) {
          return (dat[i2] ^ dat[i2 + 1] << bs1_1 ^ dat[i2 + 2] << bs2_1) & msk_1;
        };
        var syms = new i32(25e3);
        var lf = new u16(288), df = new u16(32);
        var lc_1 = 0, eb = 0, i = st.i || 0, li = 0, wi = st.w || 0, bs = 0;
        for (; i + 2 < s; ++i) {
          var hv = hsh(i);
          var imod = i & 32767, pimod = head[hv];
          prev[imod] = pimod;
          head[hv] = imod;
          if (wi <= i) {
            var rem = s - i;
            if ((lc_1 > 7e3 || li > 24576) && (rem > 423 || !lst)) {
              pos = wblk(dat, w, 0, syms, lf, df, eb, li, bs, i - bs, pos);
              li = lc_1 = eb = 0, bs = i;
              for (var j = 0; j < 286; ++j)
                lf[j] = 0;
              for (var j = 0; j < 30; ++j)
                df[j] = 0;
            }
            var l = 2, d = 0, ch_1 = c, dif = imod - pimod & 32767;
            if (rem > 2 && hv == hsh(i - dif)) {
              var maxn = Math.min(n, rem) - 1;
              var maxd = Math.min(32767, i);
              var ml = Math.min(258, rem);
              while (dif <= maxd && --ch_1 && imod != pimod) {
                if (dat[i + l] == dat[i + l - dif]) {
                  var nl = 0;
                  for (; nl < ml && dat[i + nl] == dat[i + nl - dif]; ++nl)
                    ;
                  if (nl > l) {
                    l = nl, d = dif;
                    if (nl > maxn)
                      break;
                    var mmd = Math.min(dif, nl - 2);
                    var md = 0;
                    for (var j = 0; j < mmd; ++j) {
                      var ti = i - dif + j & 32767;
                      var pti = prev[ti];
                      var cd = ti - pti & 32767;
                      if (cd > md)
                        md = cd, pimod = ti;
                    }
                  }
                }
                imod = pimod, pimod = prev[imod];
                dif += imod - pimod & 32767;
              }
            }
            if (d) {
              syms[li++] = 268435456 | revfl[l] << 18 | revfd[d];
              var lin = revfl[l] & 31, din = revfd[d] & 31;
              eb += fleb[lin] + fdeb[din];
              ++lf[257 + lin];
              ++df[din];
              wi = i + l;
              ++lc_1;
            } else {
              syms[li++] = dat[i];
              ++lf[dat[i]];
            }
          }
        }
        for (i = Math.max(i, wi); i < s; ++i) {
          syms[li++] = dat[i];
          ++lf[dat[i]];
        }
        pos = wblk(dat, w, lst, syms, lf, df, eb, li, bs, i - bs, pos);
        if (!lst) {
          st.r = pos & 7 | w[pos / 8 | 0] << 3;
          pos -= 7;
          st.h = head, st.p = prev, st.i = i, st.w = wi;
        }
      } else {
        for (var i = st.w || 0; i < s + lst; i += 65535) {
          var e = i + 65535;
          if (e >= s) {
            w[pos / 8 | 0] = lst;
            e = s;
          }
          pos = wfblk(w, pos + 1, dat.subarray(i, e));
        }
        st.i = s;
      }
      return slc(o, 0, pre + shft(pos) + post);
    };
    crct = /* @__PURE__ */ function() {
      var t = new Int32Array(256);
      for (var i = 0; i < 256; ++i) {
        var c = i, k = 9;
        while (--k)
          c = (c & 1 && -306674912) ^ c >>> 1;
        t[i] = c;
      }
      return t;
    }();
    crc = function() {
      var c = -1;
      return {
        p: function(d) {
          var cr = c;
          for (var i = 0; i < d.length; ++i)
            cr = crct[cr & 255 ^ d[i]] ^ cr >>> 8;
          c = cr;
        },
        d: function() {
          return ~c;
        }
      };
    };
    adler = function() {
      var a = 1, b = 0;
      return {
        p: function(d) {
          var n = a, m = b;
          var l = d.length | 0;
          for (var i = 0; i != l; ) {
            var e = Math.min(i + 2655, l);
            for (; i < e; ++i)
              m += n += d[i];
            n = (n & 65535) + 15 * (n >> 16), m = (m & 65535) + 15 * (m >> 16);
          }
          a = n, b = m;
        },
        d: function() {
          a %= 65521, b %= 65521;
          return (a & 255) << 24 | (a & 65280) << 8 | (b & 255) << 8 | b >> 8;
        }
      };
    };
    dopt = function(dat, opt, pre, post, st) {
      if (!st) {
        st = { l: 1 };
        if (opt.dictionary) {
          var dict = opt.dictionary.subarray(-32768);
          var newDat = new u8(dict.length + dat.length);
          newDat.set(dict);
          newDat.set(dat, dict.length);
          dat = newDat;
          st.w = dict.length;
        }
      }
      return dflt(dat, opt.level == null ? 6 : opt.level, opt.mem == null ? st.l ? Math.ceil(Math.max(8, Math.min(13, Math.log(dat.length))) * 1.5) : 20 : 12 + opt.mem, pre, post, st);
    };
    mrg = function(a, b) {
      var o = {};
      for (var k in a)
        o[k] = a[k];
      for (var k in b)
        o[k] = b[k];
      return o;
    };
    wcln = function(fn, fnStr, td2) {
      var dt = fn();
      var st = fn.toString();
      var ks = st.slice(st.indexOf("[") + 1, st.lastIndexOf("]")).replace(/\s+/g, "").split(",");
      for (var i = 0; i < dt.length; ++i) {
        var v = dt[i], k = ks[i];
        if (typeof v == "function") {
          fnStr += ";" + k + "=";
          var st_1 = v.toString();
          if (v.prototype) {
            if (st_1.indexOf("[native code]") != -1) {
              var spInd = st_1.indexOf(" ", 8) + 1;
              fnStr += st_1.slice(spInd, st_1.indexOf("(", spInd));
            } else {
              fnStr += st_1;
              for (var t in v.prototype)
                fnStr += ";" + k + ".prototype." + t + "=" + v.prototype[t].toString();
            }
          } else
            fnStr += st_1;
        } else
          td2[k] = v;
      }
      return fnStr;
    };
    ch = [];
    cbfs = function(v) {
      var tl = [];
      for (var k in v) {
        if (v[k].buffer) {
          tl.push((v[k] = new v[k].constructor(v[k])).buffer);
        }
      }
      return tl;
    };
    wrkr = function(fns, init, id, cb) {
      if (!ch[id]) {
        var fnStr = "", td_1 = {}, m = fns.length - 1;
        for (var i = 0; i < m; ++i)
          fnStr = wcln(fns[i], fnStr, td_1);
        ch[id] = { c: wcln(fns[m], fnStr, td_1), e: td_1 };
      }
      var td2 = mrg({}, ch[id].e);
      return wk(ch[id].c + ";onmessage=function(e){for(var k in e.data)self[k]=e.data[k];onmessage=" + init.toString() + "}", id, td2, cbfs(td2), cb);
    };
    bInflt = function() {
      return [u8, u16, i32, fleb, fdeb, clim, fl, fd, flrm, fdrm, rev, ec, hMap, max, bits, bits16, shft, slc, err, inflt, inflateSync, pbf, gopt];
    };
    bDflt = function() {
      return [u8, u16, i32, fleb, fdeb, clim, revfl, revfd, flm, flt, fdm, fdt, rev, deo, et, hMap, wbits, wbits16, hTree, ln, lc, clen, wfblk, wblk, shft, slc, dflt, dopt, deflateSync, pbf];
    };
    gze = function() {
      return [gzh, gzhl, wbytes, crc, crct];
    };
    guze = function() {
      return [gzs, gzl];
    };
    zle = function() {
      return [zlh, wbytes, adler];
    };
    zule = function() {
      return [zls];
    };
    pbf = function(msg) {
      return postMessage(msg, [msg.buffer]);
    };
    gopt = function(o) {
      return o && {
        out: o.size && new u8(o.size),
        dictionary: o.dictionary
      };
    };
    cbify = function(dat, opts, fns, init, id, cb) {
      var w = wrkr(fns, init, id, function(err2, dat2) {
        w.terminate();
        cb(err2, dat2);
      });
      w.postMessage([dat, opts], opts.consume ? [dat.buffer] : []);
      return function() {
        w.terminate();
      };
    };
    astrm = function(strm) {
      strm.ondata = function(dat, final) {
        return postMessage([dat, final], [dat.buffer]);
      };
      return function(ev) {
        if (ev.data.length) {
          strm.push(ev.data[0], ev.data[1]);
          postMessage([ev.data[0].length]);
        } else
          strm.flush();
      };
    };
    astrmify = function(fns, strm, opts, init, id, flush, ext) {
      var t;
      var w = wrkr(fns, init, id, function(err2, dat) {
        if (err2)
          w.terminate(), strm.ondata.call(strm, err2);
        else if (!Array.isArray(dat))
          ext(dat);
        else if (dat.length == 1) {
          strm.queuedSize -= dat[0];
          if (strm.ondrain)
            strm.ondrain(dat[0]);
        } else {
          if (dat[1])
            w.terminate();
          strm.ondata.call(strm, err2, dat[0], dat[1]);
        }
      });
      w.postMessage(opts);
      strm.queuedSize = 0;
      strm.push = function(d, f) {
        if (!strm.ondata)
          err(5);
        if (t)
          strm.ondata(err(4, 0, 1), null, !!f);
        strm.queuedSize += d.length;
        w.postMessage([d, t = f], [d.buffer]);
      };
      strm.terminate = function() {
        w.terminate();
      };
      if (flush) {
        strm.flush = function() {
          w.postMessage([]);
        };
      }
    };
    b2 = function(d, b) {
      return d[b] | d[b + 1] << 8;
    };
    b4 = function(d, b) {
      return (d[b] | d[b + 1] << 8 | d[b + 2] << 16 | d[b + 3] << 24) >>> 0;
    };
    b8 = function(d, b) {
      return b4(d, b) + b4(d, b + 4) * 4294967296;
    };
    wbytes = function(d, b, v) {
      for (; v; ++b)
        d[b] = v, v >>>= 8;
    };
    gzh = function(c, o) {
      var fn = o.filename;
      c[0] = 31, c[1] = 139, c[2] = 8, c[8] = o.level < 2 ? 4 : o.level == 9 ? 2 : 0, c[9] = 3;
      if (o.mtime != 0)
        wbytes(c, 4, Math.floor(new Date(o.mtime || Date.now()) / 1e3));
      if (fn) {
        c[3] = 8;
        for (var i = 0; i <= fn.length; ++i)
          c[i + 10] = fn.charCodeAt(i);
      }
    };
    gzs = function(d) {
      if (d[0] != 31 || d[1] != 139 || d[2] != 8)
        err(6, "invalid gzip data");
      var flg = d[3];
      var st = 10;
      if (flg & 4)
        st += (d[10] | d[11] << 8) + 2;
      for (var zs = (flg >> 3 & 1) + (flg >> 4 & 1); zs > 0; zs -= !d[st++])
        ;
      return st + (flg & 2);
    };
    gzl = function(d) {
      var l = d.length;
      return (d[l - 4] | d[l - 3] << 8 | d[l - 2] << 16 | d[l - 1] << 24) >>> 0;
    };
    gzhl = function(o) {
      return 10 + (o.filename ? o.filename.length + 1 : 0);
    };
    zlh = function(c, o) {
      var lv = o.level, fl2 = lv == 0 ? 0 : lv < 6 ? 1 : lv == 9 ? 3 : 2;
      c[0] = 120, c[1] = fl2 << 6 | (o.dictionary && 32);
      c[1] |= 31 - (c[0] << 8 | c[1]) % 31;
      if (o.dictionary) {
        var h = adler();
        h.p(o.dictionary);
        wbytes(c, 2, h.d());
      }
    };
    zls = function(d, dict) {
      if ((d[0] & 15) != 8 || d[0] >> 4 > 7 || (d[0] << 8 | d[1]) % 31)
        err(6, "invalid zlib data");
      if ((d[1] >> 5 & 1) == +!dict)
        err(6, "invalid zlib data: " + (d[1] & 32 ? "need" : "unexpected") + " dictionary");
      return (d[1] >> 3 & 4) + 2;
    };
    Deflate = /* @__PURE__ */ function() {
      function Deflate2(opts, cb) {
        if (typeof opts == "function")
          cb = opts, opts = {};
        this.ondata = cb;
        this.o = opts || {};
        this.s = { l: 0, i: 32768, w: 32768, z: 32768 };
        this.b = new u8(98304);
        if (this.o.dictionary) {
          var dict = this.o.dictionary.subarray(-32768);
          this.b.set(dict, 32768 - dict.length);
          this.s.i = 32768 - dict.length;
        }
      }
      Deflate2.prototype.p = function(c, f) {
        this.ondata(dopt(c, this.o, 0, 0, this.s), f);
      };
      Deflate2.prototype.push = function(chunk, final) {
        if (!this.ondata)
          err(5);
        if (this.s.l)
          err(4);
        var endLen = chunk.length + this.s.z;
        if (endLen > this.b.length) {
          if (endLen > 2 * this.b.length - 32768) {
            var newBuf = new u8(endLen & -32768);
            newBuf.set(this.b.subarray(0, this.s.z));
            this.b = newBuf;
          }
          var split = this.b.length - this.s.z;
          this.b.set(chunk.subarray(0, split), this.s.z);
          this.s.z = this.b.length;
          this.p(this.b, false);
          this.b.set(this.b.subarray(-32768));
          this.b.set(chunk.subarray(split), 32768);
          this.s.z = chunk.length - split + 32768;
          this.s.i = 32766, this.s.w = 32768;
        } else {
          this.b.set(chunk, this.s.z);
          this.s.z += chunk.length;
        }
        this.s.l = final & 1;
        if (this.s.z > this.s.w + 8191 || final) {
          this.p(this.b, final || false);
          this.s.w = this.s.i, this.s.i -= 2;
        }
      };
      Deflate2.prototype.flush = function() {
        if (!this.ondata)
          err(5);
        if (this.s.l)
          err(4);
        this.p(this.b, false);
        this.s.w = this.s.i, this.s.i -= 2;
      };
      return Deflate2;
    }();
    AsyncDeflate = /* @__PURE__ */ function() {
      function AsyncDeflate2(opts, cb) {
        astrmify([
          bDflt,
          function() {
            return [astrm, Deflate];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Deflate(ev.data);
          onmessage = astrm(strm);
        }, 6, 1);
      }
      return AsyncDeflate2;
    }();
    Inflate = /* @__PURE__ */ function() {
      function Inflate2(opts, cb) {
        if (typeof opts == "function")
          cb = opts, opts = {};
        this.ondata = cb;
        var dict = opts && opts.dictionary && opts.dictionary.subarray(-32768);
        this.s = { i: 0, b: dict ? dict.length : 0 };
        this.o = new u8(32768);
        this.p = new u8(0);
        if (dict)
          this.o.set(dict);
      }
      Inflate2.prototype.e = function(c) {
        if (!this.ondata)
          err(5);
        if (this.d)
          err(4);
        if (!this.p.length)
          this.p = c;
        else if (c.length) {
          var n = new u8(this.p.length + c.length);
          n.set(this.p), n.set(c, this.p.length), this.p = n;
        }
      };
      Inflate2.prototype.c = function(final) {
        this.s.i = +(this.d = final || false);
        var bts = this.s.b;
        var dt = inflt(this.p, this.s, this.o);
        this.ondata(slc(dt, bts, this.s.b), this.d);
        this.o = slc(dt, this.s.b - 32768), this.s.b = this.o.length;
        this.p = slc(this.p, this.s.p / 8 | 0), this.s.p &= 7;
      };
      Inflate2.prototype.push = function(chunk, final) {
        this.e(chunk), this.c(final);
      };
      return Inflate2;
    }();
    AsyncInflate = /* @__PURE__ */ function() {
      function AsyncInflate2(opts, cb) {
        astrmify([
          bInflt,
          function() {
            return [astrm, Inflate];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Inflate(ev.data);
          onmessage = astrm(strm);
        }, 7, 0);
      }
      return AsyncInflate2;
    }();
    Gzip = /* @__PURE__ */ function() {
      function Gzip2(opts, cb) {
        this.c = crc();
        this.l = 0;
        this.v = 1;
        Deflate.call(this, opts, cb);
      }
      Gzip2.prototype.push = function(chunk, final) {
        this.c.p(chunk);
        this.l += chunk.length;
        Deflate.prototype.push.call(this, chunk, final);
      };
      Gzip2.prototype.p = function(c, f) {
        var raw = dopt(c, this.o, this.v && gzhl(this.o), f && 8, this.s);
        if (this.v)
          gzh(raw, this.o), this.v = 0;
        if (f)
          wbytes(raw, raw.length - 8, this.c.d()), wbytes(raw, raw.length - 4, this.l);
        this.ondata(raw, f);
      };
      Gzip2.prototype.flush = function() {
        Deflate.prototype.flush.call(this);
      };
      return Gzip2;
    }();
    AsyncGzip = /* @__PURE__ */ function() {
      function AsyncGzip2(opts, cb) {
        astrmify([
          bDflt,
          gze,
          function() {
            return [astrm, Deflate, Gzip];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Gzip(ev.data);
          onmessage = astrm(strm);
        }, 8, 1);
      }
      return AsyncGzip2;
    }();
    Gunzip = /* @__PURE__ */ function() {
      function Gunzip2(opts, cb) {
        this.v = 1;
        this.r = 0;
        Inflate.call(this, opts, cb);
      }
      Gunzip2.prototype.push = function(chunk, final) {
        Inflate.prototype.e.call(this, chunk);
        this.r += chunk.length;
        if (this.v) {
          var p = this.p.subarray(this.v - 1);
          var s = p.length > 3 ? gzs(p) : 4;
          if (s > p.length) {
            if (!final)
              return;
          } else if (this.v > 1 && this.onmember) {
            this.onmember(this.r - p.length);
          }
          this.p = p.subarray(s), this.v = 0;
        }
        Inflate.prototype.c.call(this, final);
        if (this.s.f && !this.s.l && !final) {
          this.v = shft(this.s.p) + 9;
          this.s = { i: 0 };
          this.o = new u8(0);
          this.push(new u8(0), final);
        }
      };
      return Gunzip2;
    }();
    AsyncGunzip = /* @__PURE__ */ function() {
      function AsyncGunzip2(opts, cb) {
        var _this = this;
        astrmify([
          bInflt,
          guze,
          function() {
            return [astrm, Inflate, Gunzip];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Gunzip(ev.data);
          strm.onmember = function(offset) {
            return postMessage(offset);
          };
          onmessage = astrm(strm);
        }, 9, 0, function(offset) {
          return _this.onmember && _this.onmember(offset);
        });
      }
      return AsyncGunzip2;
    }();
    Zlib = /* @__PURE__ */ function() {
      function Zlib2(opts, cb) {
        this.c = adler();
        this.v = 1;
        Deflate.call(this, opts, cb);
      }
      Zlib2.prototype.push = function(chunk, final) {
        this.c.p(chunk);
        Deflate.prototype.push.call(this, chunk, final);
      };
      Zlib2.prototype.p = function(c, f) {
        var raw = dopt(c, this.o, this.v && (this.o.dictionary ? 6 : 2), f && 4, this.s);
        if (this.v)
          zlh(raw, this.o), this.v = 0;
        if (f)
          wbytes(raw, raw.length - 4, this.c.d());
        this.ondata(raw, f);
      };
      Zlib2.prototype.flush = function() {
        Deflate.prototype.flush.call(this);
      };
      return Zlib2;
    }();
    AsyncZlib = /* @__PURE__ */ function() {
      function AsyncZlib2(opts, cb) {
        astrmify([
          bDflt,
          zle,
          function() {
            return [astrm, Deflate, Zlib];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Zlib(ev.data);
          onmessage = astrm(strm);
        }, 10, 1);
      }
      return AsyncZlib2;
    }();
    Unzlib = /* @__PURE__ */ function() {
      function Unzlib2(opts, cb) {
        Inflate.call(this, opts, cb);
        this.v = opts && opts.dictionary ? 2 : 1;
      }
      Unzlib2.prototype.push = function(chunk, final) {
        Inflate.prototype.e.call(this, chunk);
        if (this.v) {
          if (this.p.length < 6 && !final)
            return;
          this.p = this.p.subarray(zls(this.p, this.v - 1)), this.v = 0;
        }
        if (final) {
          if (this.p.length < 4)
            err(6, "invalid zlib data");
          this.p = this.p.subarray(0, -4);
        }
        Inflate.prototype.c.call(this, final);
      };
      return Unzlib2;
    }();
    AsyncUnzlib = /* @__PURE__ */ function() {
      function AsyncUnzlib2(opts, cb) {
        astrmify([
          bInflt,
          zule,
          function() {
            return [astrm, Inflate, Unzlib];
          }
        ], this, StrmOpt.call(this, opts, cb), function(ev) {
          var strm = new Unzlib(ev.data);
          onmessage = astrm(strm);
        }, 11, 0);
      }
      return AsyncUnzlib2;
    }();
    Decompress = /* @__PURE__ */ function() {
      function Decompress2(opts, cb) {
        this.o = StrmOpt.call(this, opts, cb) || {};
        this.G = Gunzip;
        this.I = Inflate;
        this.Z = Unzlib;
      }
      Decompress2.prototype.i = function() {
        var _this = this;
        this.s.ondata = function(dat, final) {
          _this.ondata(dat, final);
        };
      };
      Decompress2.prototype.push = function(chunk, final) {
        if (!this.ondata)
          err(5);
        if (!this.s) {
          if (this.p && this.p.length) {
            var n = new u8(this.p.length + chunk.length);
            n.set(this.p), n.set(chunk, this.p.length);
          } else
            this.p = chunk;
          if (this.p.length > 2) {
            this.s = this.p[0] == 31 && this.p[1] == 139 && this.p[2] == 8 ? new this.G(this.o) : (this.p[0] & 15) != 8 || this.p[0] >> 4 > 7 || (this.p[0] << 8 | this.p[1]) % 31 ? new this.I(this.o) : new this.Z(this.o);
            this.i();
            this.s.push(this.p, final);
            this.p = null;
          }
        } else
          this.s.push(chunk, final);
      };
      return Decompress2;
    }();
    AsyncDecompress = /* @__PURE__ */ function() {
      function AsyncDecompress2(opts, cb) {
        Decompress.call(this, opts, cb);
        this.queuedSize = 0;
        this.G = AsyncGunzip;
        this.I = AsyncInflate;
        this.Z = AsyncUnzlib;
      }
      AsyncDecompress2.prototype.i = function() {
        var _this = this;
        this.s.ondata = function(err2, dat, final) {
          _this.ondata(err2, dat, final);
        };
        this.s.ondrain = function(size) {
          _this.queuedSize -= size;
          if (_this.ondrain)
            _this.ondrain(size);
        };
      };
      AsyncDecompress2.prototype.push = function(chunk, final) {
        this.queuedSize += chunk.length;
        Decompress.prototype.push.call(this, chunk, final);
      };
      return AsyncDecompress2;
    }();
    fltn = function(d, p, t, o) {
      for (var k in d) {
        var val = d[k], n = p + k, op = o;
        if (Array.isArray(val))
          op = mrg(o, val[1]), val = val[0];
        if (val instanceof u8)
          t[n] = [val, op];
        else {
          t[n += "/"] = [new u8(0), op];
          fltn(val, n, t, o);
        }
      }
    };
    te = typeof TextEncoder != "undefined" && /* @__PURE__ */ new TextEncoder();
    td = typeof TextDecoder != "undefined" && /* @__PURE__ */ new TextDecoder();
    tds = 0;
    try {
      td.decode(et, { stream: true });
      tds = 1;
    } catch (e) {
    }
    dutf8 = function(d) {
      for (var r = "", i = 0; ; ) {
        var c = d[i++];
        var eb = (c > 127) + (c > 223) + (c > 239);
        if (i + eb > d.length)
          return { s: r, r: slc(d, i - 1) };
        if (!eb)
          r += String.fromCharCode(c);
        else if (eb == 3) {
          c = ((c & 15) << 18 | (d[i++] & 63) << 12 | (d[i++] & 63) << 6 | d[i++] & 63) - 65536, r += String.fromCharCode(55296 | c >> 10, 56320 | c & 1023);
        } else if (eb & 1)
          r += String.fromCharCode((c & 31) << 6 | d[i++] & 63);
        else
          r += String.fromCharCode((c & 15) << 12 | (d[i++] & 63) << 6 | d[i++] & 63);
      }
    };
    DecodeUTF8 = /* @__PURE__ */ function() {
      function DecodeUTF82(cb) {
        this.ondata = cb;
        if (tds)
          this.t = new TextDecoder();
        else
          this.p = et;
      }
      DecodeUTF82.prototype.push = function(chunk, final) {
        if (!this.ondata)
          err(5);
        final = !!final;
        if (this.t) {
          this.ondata(this.t.decode(chunk, { stream: true }), final);
          if (final) {
            if (this.t.decode().length)
              err(8);
            this.t = null;
          }
          return;
        }
        if (!this.p)
          err(4);
        var dat = new u8(this.p.length + chunk.length);
        dat.set(this.p);
        dat.set(chunk, this.p.length);
        var _a2 = dutf8(dat), s = _a2.s, r = _a2.r;
        if (final) {
          if (r.length)
            err(8);
          this.p = null;
        } else
          this.p = r;
        this.ondata(s, final);
      };
      return DecodeUTF82;
    }();
    EncodeUTF8 = /* @__PURE__ */ function() {
      function EncodeUTF82(cb) {
        this.ondata = cb;
      }
      EncodeUTF82.prototype.push = function(chunk, final) {
        if (!this.ondata)
          err(5);
        if (this.d)
          err(4);
        this.ondata(strToU8(chunk), this.d = final || false);
      };
      return EncodeUTF82;
    }();
    dbf = function(l) {
      return l == 1 ? 3 : l < 6 ? 2 : l == 9 ? 1 : 0;
    };
    slzh = function(d, b) {
      return b + 30 + b2(d, b + 26) + b2(d, b + 28);
    };
    zh = function(d, b, z) {
      var fnl = b2(d, b + 28), fn = strFromU8(d.subarray(b + 46, b + 46 + fnl), !(b2(d, b + 8) & 2048)), es = b + 46 + fnl, bs = b4(d, b + 20);
      var _a2 = z && bs == 4294967295 ? z64e(d, es) : [bs, b4(d, b + 24), b4(d, b + 42)], sc = _a2[0], su = _a2[1], off = _a2[2];
      return [b2(d, b + 10), sc, su, fn, es + b2(d, b + 30) + b2(d, b + 32), off];
    };
    z64e = function(d, b) {
      for (; b2(d, b) != 1; b += 4 + b2(d, b + 2))
        ;
      return [b8(d, b + 12), b8(d, b + 4), b8(d, b + 20)];
    };
    exfl = function(ex) {
      var le = 0;
      if (ex) {
        for (var k in ex) {
          var l = ex[k].length;
          if (l > 65535)
            err(9);
          le += l + 4;
        }
      }
      return le;
    };
    wzh = function(d, b, f, fn, u, c, ce, co) {
      var fl2 = fn.length, ex = f.extra, col = co && co.length;
      var exl = exfl(ex);
      wbytes(d, b, ce != null ? 33639248 : 67324752), b += 4;
      if (ce != null)
        d[b++] = 20, d[b++] = f.os;
      d[b] = 20, b += 2;
      d[b++] = f.flag << 1 | (c < 0 && 8), d[b++] = u && 8;
      d[b++] = f.compression & 255, d[b++] = f.compression >> 8;
      var dt = new Date(f.mtime == null ? Date.now() : f.mtime), y = dt.getFullYear() - 1980;
      if (y < 0 || y > 119)
        err(10);
      wbytes(d, b, y << 25 | dt.getMonth() + 1 << 21 | dt.getDate() << 16 | dt.getHours() << 11 | dt.getMinutes() << 5 | dt.getSeconds() >> 1), b += 4;
      if (c != -1) {
        wbytes(d, b, f.crc);
        wbytes(d, b + 4, c < 0 ? -c - 2 : c);
        wbytes(d, b + 8, f.size);
      }
      wbytes(d, b + 12, fl2);
      wbytes(d, b + 14, exl), b += 16;
      if (ce != null) {
        wbytes(d, b, col);
        wbytes(d, b + 6, f.attrs);
        wbytes(d, b + 10, ce), b += 14;
      }
      d.set(fn, b);
      b += fl2;
      if (exl) {
        for (var k in ex) {
          var exf = ex[k], l = exf.length;
          wbytes(d, b, +k);
          wbytes(d, b + 2, l);
          d.set(exf, b + 4), b += 4 + l;
        }
      }
      if (col)
        d.set(co, b), b += col;
      return b;
    };
    wzf = function(o, b, c, d, e) {
      wbytes(o, b, 101010256);
      wbytes(o, b + 8, c);
      wbytes(o, b + 10, c);
      wbytes(o, b + 12, d);
      wbytes(o, b + 16, e);
    };
    ZipPassThrough = /* @__PURE__ */ function() {
      function ZipPassThrough2(filename) {
        this.filename = filename;
        this.c = crc();
        this.size = 0;
        this.compression = 0;
      }
      ZipPassThrough2.prototype.process = function(chunk, final) {
        this.ondata(null, chunk, final);
      };
      ZipPassThrough2.prototype.push = function(chunk, final) {
        if (!this.ondata)
          err(5);
        this.c.p(chunk);
        this.size += chunk.length;
        if (final)
          this.crc = this.c.d();
        this.process(chunk, final || false);
      };
      return ZipPassThrough2;
    }();
    ZipDeflate = /* @__PURE__ */ function() {
      function ZipDeflate2(filename, opts) {
        var _this = this;
        if (!opts)
          opts = {};
        ZipPassThrough.call(this, filename);
        this.d = new Deflate(opts, function(dat, final) {
          _this.ondata(null, dat, final);
        });
        this.compression = 8;
        this.flag = dbf(opts.level);
      }
      ZipDeflate2.prototype.process = function(chunk, final) {
        try {
          this.d.push(chunk, final);
        } catch (e) {
          this.ondata(e, null, final);
        }
      };
      ZipDeflate2.prototype.push = function(chunk, final) {
        ZipPassThrough.prototype.push.call(this, chunk, final);
      };
      return ZipDeflate2;
    }();
    AsyncZipDeflate = /* @__PURE__ */ function() {
      function AsyncZipDeflate2(filename, opts) {
        var _this = this;
        if (!opts)
          opts = {};
        ZipPassThrough.call(this, filename);
        this.d = new AsyncDeflate(opts, function(err2, dat, final) {
          _this.ondata(err2, dat, final);
        });
        this.compression = 8;
        this.flag = dbf(opts.level);
        this.terminate = this.d.terminate;
      }
      AsyncZipDeflate2.prototype.process = function(chunk, final) {
        this.d.push(chunk, final);
      };
      AsyncZipDeflate2.prototype.push = function(chunk, final) {
        ZipPassThrough.prototype.push.call(this, chunk, final);
      };
      return AsyncZipDeflate2;
    }();
    Zip = /* @__PURE__ */ function() {
      function Zip2(cb) {
        this.ondata = cb;
        this.u = [];
        this.d = 1;
      }
      Zip2.prototype.add = function(file) {
        var _this = this;
        if (!this.ondata)
          err(5);
        if (this.d & 2)
          this.ondata(err(4 + (this.d & 1) * 8, 0, 1), null, false);
        else {
          var f = strToU8(file.filename), fl_1 = f.length;
          var com = file.comment, o = com && strToU8(com);
          var u = fl_1 != file.filename.length || o && com.length != o.length;
          var hl_1 = fl_1 + exfl(file.extra) + 30;
          if (fl_1 > 65535)
            this.ondata(err(11, 0, 1), null, false);
          var header = new u8(hl_1);
          wzh(header, 0, file, f, u, -1);
          var chks_1 = [header];
          var pAll_1 = function() {
            for (var _i = 0, chks_2 = chks_1; _i < chks_2.length; _i++) {
              var chk = chks_2[_i];
              _this.ondata(null, chk, false);
            }
            chks_1 = [];
          };
          var tr_1 = this.d;
          this.d = 0;
          var ind_1 = this.u.length;
          var uf_1 = mrg(file, {
            f,
            u,
            o,
            t: function() {
              if (file.terminate)
                file.terminate();
            },
            r: function() {
              pAll_1();
              if (tr_1) {
                var nxt = _this.u[ind_1 + 1];
                if (nxt)
                  nxt.r();
                else
                  _this.d = 1;
              }
              tr_1 = 1;
            }
          });
          var cl_1 = 0;
          file.ondata = function(err2, dat, final) {
            if (err2) {
              _this.ondata(err2, dat, final);
              _this.terminate();
            } else {
              cl_1 += dat.length;
              chks_1.push(dat);
              if (final) {
                var dd = new u8(16);
                wbytes(dd, 0, 134695760);
                wbytes(dd, 4, file.crc);
                wbytes(dd, 8, cl_1);
                wbytes(dd, 12, file.size);
                chks_1.push(dd);
                uf_1.c = cl_1, uf_1.b = hl_1 + cl_1 + 16, uf_1.crc = file.crc, uf_1.size = file.size;
                if (tr_1)
                  uf_1.r();
                tr_1 = 1;
              } else if (tr_1)
                pAll_1();
            }
          };
          this.u.push(uf_1);
        }
      };
      Zip2.prototype.end = function() {
        var _this = this;
        if (this.d & 2) {
          this.ondata(err(4 + (this.d & 1) * 8, 0, 1), null, true);
          return;
        }
        if (this.d)
          this.e();
        else
          this.u.push({
            r: function() {
              if (!(_this.d & 1))
                return;
              _this.u.splice(-1, 1);
              _this.e();
            },
            t: function() {
            }
          });
        this.d = 3;
      };
      Zip2.prototype.e = function() {
        var bt = 0, l = 0, tl = 0;
        for (var _i = 0, _a2 = this.u; _i < _a2.length; _i++) {
          var f = _a2[_i];
          tl += 46 + f.f.length + exfl(f.extra) + (f.o ? f.o.length : 0);
        }
        var out = new u8(tl + 22);
        for (var _b2 = 0, _c = this.u; _b2 < _c.length; _b2++) {
          var f = _c[_b2];
          wzh(out, bt, f, f.f, f.u, -f.c - 2, l, f.o);
          bt += 46 + f.f.length + exfl(f.extra) + (f.o ? f.o.length : 0), l += f.b;
        }
        wzf(out, bt, this.u.length, tl, l);
        this.ondata(null, out, true);
        this.d = 2;
      };
      Zip2.prototype.terminate = function() {
        for (var _i = 0, _a2 = this.u; _i < _a2.length; _i++) {
          var f = _a2[_i];
          f.t();
        }
        this.d = 2;
      };
      return Zip2;
    }();
    UnzipPassThrough = /* @__PURE__ */ function() {
      function UnzipPassThrough2() {
      }
      UnzipPassThrough2.prototype.push = function(data, final) {
        this.ondata(null, data, final);
      };
      UnzipPassThrough2.compression = 0;
      return UnzipPassThrough2;
    }();
    UnzipInflate = /* @__PURE__ */ function() {
      function UnzipInflate2() {
        var _this = this;
        this.i = new Inflate(function(dat, final) {
          _this.ondata(null, dat, final);
        });
      }
      UnzipInflate2.prototype.push = function(data, final) {
        try {
          this.i.push(data, final);
        } catch (e) {
          this.ondata(e, null, final);
        }
      };
      UnzipInflate2.compression = 8;
      return UnzipInflate2;
    }();
    AsyncUnzipInflate = /* @__PURE__ */ function() {
      function AsyncUnzipInflate2(_, sz) {
        var _this = this;
        if (sz < 32e4) {
          this.i = new Inflate(function(dat, final) {
            _this.ondata(null, dat, final);
          });
        } else {
          this.i = new AsyncInflate(function(err2, dat, final) {
            _this.ondata(err2, dat, final);
          });
          this.terminate = this.i.terminate;
        }
      }
      AsyncUnzipInflate2.prototype.push = function(data, final) {
        if (this.i.terminate)
          data = slc(data, 0);
        this.i.push(data, final);
      };
      AsyncUnzipInflate2.compression = 8;
      return AsyncUnzipInflate2;
    }();
    Unzip = /* @__PURE__ */ function() {
      function Unzip2(cb) {
        this.onfile = cb;
        this.k = [];
        this.o = {
          0: UnzipPassThrough
        };
        this.p = et;
      }
      Unzip2.prototype.push = function(chunk, final) {
        var _this = this;
        if (!this.onfile)
          err(5);
        if (!this.p)
          err(4);
        if (this.c > 0) {
          var len = Math.min(this.c, chunk.length);
          var toAdd = chunk.subarray(0, len);
          this.c -= len;
          if (this.d)
            this.d.push(toAdd, !this.c);
          else
            this.k[0].push(toAdd);
          chunk = chunk.subarray(len);
          if (chunk.length)
            return this.push(chunk, final);
        } else {
          var f = 0, i = 0, is = void 0, buf = void 0;
          if (!this.p.length)
            buf = chunk;
          else if (!chunk.length)
            buf = this.p;
          else {
            buf = new u8(this.p.length + chunk.length);
            buf.set(this.p), buf.set(chunk, this.p.length);
          }
          var l = buf.length, oc = this.c, add = oc && this.d;
          var _loop_2 = function() {
            var _a2;
            var sig = b4(buf, i);
            if (sig == 67324752) {
              f = 1, is = i;
              this_1.d = null;
              this_1.c = 0;
              var bf = b2(buf, i + 6), cmp_1 = b2(buf, i + 8), u = bf & 2048, dd = bf & 8, fnl = b2(buf, i + 26), es = b2(buf, i + 28);
              if (l > i + 30 + fnl + es) {
                var chks_3 = [];
                this_1.k.unshift(chks_3);
                f = 2;
                var sc_1 = b4(buf, i + 18), su_1 = b4(buf, i + 22);
                var fn_1 = strFromU8(buf.subarray(i + 30, i += 30 + fnl), !u);
                if (sc_1 == 4294967295) {
                  _a2 = dd ? [-2] : z64e(buf, i), sc_1 = _a2[0], su_1 = _a2[1];
                } else if (dd)
                  sc_1 = -1;
                i += es;
                this_1.c = sc_1;
                var d_1;
                var file_1 = {
                  name: fn_1,
                  compression: cmp_1,
                  start: function() {
                    if (!file_1.ondata)
                      err(5);
                    if (!sc_1)
                      file_1.ondata(null, et, true);
                    else {
                      var ctr = _this.o[cmp_1];
                      if (!ctr)
                        file_1.ondata(err(14, "unknown compression type " + cmp_1, 1), null, false);
                      d_1 = sc_1 < 0 ? new ctr(fn_1) : new ctr(fn_1, sc_1, su_1);
                      d_1.ondata = function(err2, dat3, final2) {
                        file_1.ondata(err2, dat3, final2);
                      };
                      for (var _i = 0, chks_4 = chks_3; _i < chks_4.length; _i++) {
                        var dat2 = chks_4[_i];
                        d_1.push(dat2, false);
                      }
                      if (_this.k[0] == chks_3 && _this.c)
                        _this.d = d_1;
                      else
                        d_1.push(et, true);
                    }
                  },
                  terminate: function() {
                    if (d_1 && d_1.terminate)
                      d_1.terminate();
                  }
                };
                if (sc_1 >= 0)
                  file_1.size = sc_1, file_1.originalSize = su_1;
                this_1.onfile(file_1);
              }
              return "break";
            } else if (oc) {
              if (sig == 134695760) {
                is = i += 12 + (oc == -2 && 8), f = 3, this_1.c = 0;
                return "break";
              } else if (sig == 33639248) {
                is = i -= 4, f = 3, this_1.c = 0;
                return "break";
              }
            }
          };
          var this_1 = this;
          for (; i < l - 4; ++i) {
            var state_1 = _loop_2();
            if (state_1 === "break")
              break;
          }
          this.p = et;
          if (oc < 0) {
            var dat = f ? buf.subarray(0, is - 12 - (oc == -2 && 8) - (b4(buf, is - 16) == 134695760 && 4)) : buf.subarray(0, i);
            if (add)
              add.push(dat, !!f);
            else
              this.k[+(f == 2)].push(dat);
          }
          if (f & 2)
            return this.push(buf.subarray(i), final);
          this.p = buf.subarray(i);
        }
        if (final) {
          if (this.c)
            err(13);
          this.p = null;
        }
      };
      Unzip2.prototype.register = function(decoder) {
        this.o[decoder.compression] = decoder;
      };
      return Unzip2;
    }();
    mt = typeof queueMicrotask == "function" ? queueMicrotask : typeof setTimeout == "function" ? setTimeout : function(fn) {
      fn();
    };
  }
});

// src/scripts/core/eventBus.ts
var subscribers = /* @__PURE__ */ new Map();
var counter = 0;
function subscribe(eventName, handler) {
  if (!subscribers.has(eventName)) {
    subscribers.set(eventName, /* @__PURE__ */ new Map());
  }
  const token = `h_${eventName}_${counter++}`;
  subscribers.get(eventName).set(token, handler);
  return () => {
    const handlers = subscribers.get(eventName);
    if (handlers) {
      handlers.delete(token);
      if (!handlers.size) {
        subscribers.delete(eventName);
      }
    }
  };
}
function emit(eventName, payload) {
  const handlers = subscribers.get(eventName);
  if (!handlers) {
    return;
  }
  for (const handler of handlers.values()) {
    try {
      handler(payload);
    } catch (err2) {
      console.error(`eventBus handler error for ${eventName}`, err2);
    }
  }
}

// src/scripts/core/labels.ts
var DEFAULT_FIELD_LABELS = {
  calculation: {
    fields: {
      creator: {
        label: "Erstellt von",
        placeholder: "Name der verantwortlichen Person"
      },
      location: {
        label: "Standort / Abteil",
        placeholder: "z. B. Gew\xE4chshaus 1"
      },
      crop: {
        label: "Kultur",
        placeholder: "z. B. Salat"
      },
      quantity: {
        label: "Anzahl Kisten",
        placeholder: "z. B. 42",
        unit: "Kisten"
      }
    },
    summary: {
      water: "Gesamtwasser (L)",
      area: "Fl\xE4che (Ar / m\xB2)"
    },
    tableColumns: {
      medium: "Mittel",
      unit: "Einheit",
      method: "Methode",
      value: "Wert",
      perQuantity: "Kisten",
      areaAr: "Ar",
      areaSqm: "m\xB2",
      total: "Gesamt"
    },
    resultTitle: "Ben\xF6tigte Mittel"
  },
  history: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten"
    },
    detail: {
      title: "Historieneintrag",
      creator: "Erstellt von",
      location: "Standort / Abteil",
      crop: "Kultur",
      quantity: "Kisten"
    },
    summaryTitle: "Historie (Zusammenfassung)",
    mediumsHeading: "Mittel & Gesamtmengen"
  },
  reporting: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten",
      mediums: "Mittel & Gesamtmengen"
    },
    infoAll: "Alle Eintr\xE4ge",
    infoEmpty: "Keine Eintr\xE4ge vorhanden",
    infoPrefix: "Zeitraum",
    printTitle: "Auswertung"
  }
};
function deepMerge(target, source) {
  const result = Array.isArray(target) ? [...target] : { ...target };
  if (!source || typeof source !== "object") {
    return result;
  }
  for (const [key, value] of Object.entries(source)) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const baseValue = key in result ? result[key] : {};
      result[key] = deepMerge(baseValue, value);
    } else {
      result[key] = Array.isArray(value) ? [...value] : value;
    }
  }
  return result;
}
function cloneLabels(labels) {
  return JSON.parse(JSON.stringify(labels));
}
function getDefaultFieldLabels() {
  return cloneLabels(DEFAULT_FIELD_LABELS);
}
function resolveFieldLabels(custom = {}) {
  return deepMerge(getDefaultFieldLabels(), custom);
}
function setFieldLabelByPath(labels, path, value) {
  const segments = path.split(".");
  const copy = cloneLabels(labels);
  let cursor = copy;
  for (let i = 0; i < segments.length; i += 1) {
    const segment = segments[i];
    if (i === segments.length - 1) {
      cursor[segment] = value;
    } else {
      cursor[segment] = cursor[segment] && typeof cursor[segment] === "object" ? { ...cursor[segment] } : {};
      cursor = cursor[segment];
    }
  }
  return resolveFieldLabels(copy);
}

// src/scripts/core/state.ts
var listeners = /* @__PURE__ */ new Set();
var state = {
  app: {
    ready: false,
    version: null,
    hasFileAccess: false,
    hasDatabase: false,
    activeSection: "calc",
    storageDriver: "memory"
  },
  company: {
    name: "",
    headline: "",
    logoUrl: "",
    contactEmail: "",
    address: "",
    accentColor: ""
  },
  defaults: {
    waterPerKisteL: 5,
    kistenProAr: 300,
    form: {
      creator: "",
      location: "",
      crop: "",
      quantity: ""
    }
  },
  measurementMethods: [],
  mediums: [],
  history: [],
  fieldLabels: getDefaultFieldLabels(),
  calcContext: null,
  zulassung: {
    filters: {
      culture: null,
      pest: null,
      text: "",
      includeExpired: false
    },
    results: [],
    lastSync: null,
    lastResultCounts: null,
    dataSource: null,
    apiStand: null,
    manifestVersion: null,
    lastSyncHash: null,
    busy: false,
    progress: { step: null, percent: 0, message: "" },
    error: null,
    logs: [],
    debug: {
      schema: null,
      lastSyncLog: [],
      manifest: null,
      lastAutoUpdateCheck: null
    },
    lookups: { cultures: [], pests: [] },
    autoUpdateAvailable: false,
    autoUpdateVersion: null
  },
  ui: {
    notifications: []
  }
};
function getState() {
  return state;
}
function subscribeState(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
function notify(prevState) {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err2) {
      console.error("state listener error", err2);
    }
  }
}
function patchState(patch) {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}
function updateSlice(sliceKey, updater) {
  const currentSlice = state[sliceKey];
  const nextSlice = typeof updater === "function" ? updater(
    currentSlice,
    state
  ) : updater;
  if (nextSlice === currentSlice) {
    return state;
  }
  return patchState({ [sliceKey]: nextSlice });
}

// src/scripts/core/utils.ts
function escapeHtml(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function formatNumber(value, fractionDigits = 2, fallback = "\u2013") {
  const num = Number.parseFloat(value);
  if (Number.isNaN(num)) {
    return fallback;
  }
  return num.toFixed(fractionDigits);
}
function nextFrame() {
  return new Promise((resolve) => requestAnimationFrame(resolve));
}

// src/scripts/core/config.ts
var cachedDefaults = null;
function getDefaultsConfig() {
  return cachedDefaults;
}

// src/scripts/core/database.ts
function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}
function deepMerge2(target, source) {
  if (!source || typeof source !== "object") {
    return target;
  }
  const result = Array.isArray(target) ? [...target] : { ...target };
  for (const [key, value] of Object.entries(source)) {
    if (Array.isArray(value)) {
      result[key] = value.map((item) => item && typeof item === "object" ? clone(item) : item);
      continue;
    }
    if (value && typeof value === "object") {
      const baseValue = result[key] && typeof result[key] === "object" && !Array.isArray(result[key]) ? result[key] : {};
      result[key] = deepMerge2(baseValue, value);
      continue;
    }
    result[key] = value;
  }
  return result;
}
function mergeDefaults(base = {}, incoming = {}) {
  const merged = { ...base, ...incoming };
  merged.form = {
    ...base.form || { creator: "", location: "", crop: "", quantity: "" },
    ...incoming.form || {}
  };
  return merged;
}
function applyDatabase(data) {
  if (!data) {
    throw new Error("Keine Daten zum Anwenden \xFCbergeben");
  }
  const current = getState();
  const fieldLabels = resolveFieldLabels(data.meta?.fieldLabels ?? {});
  patchState({
    company: { ...current.company, ...data.meta?.company ?? {} },
    defaults: mergeDefaults(current.defaults, data.meta?.defaults ?? {}),
    measurementMethods: [...data.meta?.measurementMethods ?? current.measurementMethods],
    mediums: [...data.mediums ?? []],
    history: [...data.history ?? []],
    fieldLabels,
    app: {
      ...current.app,
      hasDatabase: true
    }
  });
}
function createInitialDatabase(overrides = {}) {
  const defaults = getDefaultsConfig();
  if (defaults) {
    const base2 = clone(defaults);
    base2.meta = base2.meta || {};
    base2.meta.fieldLabels = resolveFieldLabels(base2.meta.fieldLabels ?? {});
    base2.meta.defaults = mergeDefaults(
      {
        waterPerKisteL: 5,
        kistenProAr: 300,
        form: {
          creator: "",
          location: "",
          crop: "",
          quantity: ""
        }
      },
      base2.meta.defaults ?? {}
    );
    return deepMerge2(base2, overrides);
  }
  const state2 = getState();
  const base = {
    meta: {
      version: state2.app.version || 1,
      company: { ...state2.company },
      defaults: { ...state2.defaults },
      measurementMethods: [...state2.measurementMethods],
      fieldLabels: { ...state2.fieldLabels }
    },
    mediums: [...state2.mediums],
    history: []
  };
  return deepMerge2(base, overrides);
}
function getDatabaseSnapshot() {
  const state2 = getState();
  return {
    meta: {
      version: state2.app.version || 1,
      company: { ...state2.company },
      defaults: { ...state2.defaults },
      measurementMethods: [...state2.measurementMethods],
      fieldLabels: { ...state2.fieldLabels }
    },
    mediums: [...state2.mediums],
    history: [...state2.history]
  };
}

// src/scripts/core/storage/fileSystem.ts
var fileSystem_exports = {};
__export(fileSystem_exports, {
  create: () => create,
  getContext: () => getContext,
  isSupported: () => isSupported,
  open: () => open,
  reset: () => reset,
  save: () => save
});
var FILE_OPTIONS = {
  description: "JSON-Datei",
  accept: { "application/json": [".json"] }
};
var fileHandle = null;
async function writeFile(handle, data) {
  const writable = await handle.createWritable();
  await writable.write(JSON.stringify(data, null, 2));
  await writable.close();
}
async function readFile(handle) {
  const file = await handle.getFile();
  return file.text();
}
function isSupported() {
  return typeof window !== "undefined" && typeof window.showSaveFilePicker === "function";
}
async function create(initialData, suggestedName = "database.json") {
  if (!isSupported()) {
    throw new Error("File System Access API not available");
  }
  fileHandle = await window.showSaveFilePicker({
    suggestedName,
    types: [FILE_OPTIONS]
  });
  await writeFile(fileHandle, initialData);
  return { data: initialData, context: { fileHandle } };
}
async function open() {
  if (!isSupported()) {
    throw new Error("File System Access API not available");
  }
  const [handle] = await window.showOpenFilePicker({ types: [FILE_OPTIONS] });
  fileHandle = handle;
  const text = await readFile(fileHandle);
  const data = JSON.parse(text);
  return { data, context: { fileHandle } };
}
async function save(data) {
  if (!fileHandle) {
    throw new Error("Kein Dateihandle vorhanden");
  }
  await writeFile(fileHandle, data);
  return { context: { fileHandle } };
}
function getContext() {
  return { fileHandle };
}
function reset() {
  fileHandle = null;
}

// src/scripts/core/storage/fallback.ts
var fallback_exports = {};
__export(fallback_exports, {
  create: () => create2,
  getContext: () => getContext2,
  isSupported: () => isSupported2,
  open: () => open2,
  reset: () => reset2,
  save: () => save2
});
var STORAGE_KEY = "pflanzenschutzliste-db";
function hasLocalStorage() {
  try {
    const key = "__storage_test__";
    window.localStorage.setItem(key, "1");
    window.localStorage.removeItem(key);
    return true;
  } catch (err2) {
    return false;
  }
}
function isSupported2() {
  return typeof window !== "undefined" && hasLocalStorage();
}
async function create2(initialData) {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(initialData));
  return { data: initialData, context: { storageKey: STORAGE_KEY } };
}
async function open2() {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    throw new Error("Keine gespeicherten Daten gefunden");
  }
  return { data: JSON.parse(raw), context: { storageKey: STORAGE_KEY } };
}
async function save2(data) {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
  return { context: { storageKey: STORAGE_KEY } };
}
function getContext2() {
  return { storageKey: STORAGE_KEY };
}
function reset2() {
  if (!isSupported2()) {
    return;
  }
  window.localStorage.removeItem(STORAGE_KEY);
}

// src/scripts/core/storage/sqlite.ts
var sqlite_exports = {};
__export(sqlite_exports, {
  appendBvlSyncLog: () => appendBvlSyncLog,
  create: () => create3,
  diagnoseBvlSchema: () => diagnoseBvlSchema,
  exec: () => exec,
  exportDB: () => exportDB,
  exportSnapshot: () => exportSnapshot,
  getBvlMeta: () => getBvlMeta,
  getBvlSyncStatus: () => getBvlSyncStatus,
  getContext: () => getContext3,
  importBvlDataset: () => importBvlDataset,
  importBvlSqlite: () => importBvlSqlite,
  importDB: () => importDB,
  importSnapshot: () => importSnapshot,
  isSupported: () => isSupported3,
  listBvlCultures: () => listBvlCultures,
  listBvlSchadorg: () => listBvlSchadorg,
  listBvlSyncLog: () => listBvlSyncLog,
  open: () => open3,
  query: () => query,
  queryBvl: () => queryBvl,
  queryZulassung: () => queryZulassung,
  reset: () => reset3,
  save: () => save3,
  setBvlMeta: () => setBvlMeta
});
var worker = null;
var messageId = 0;
var pendingMessages = /* @__PURE__ */ new Map();
var fileHandle2 = null;
function isSupported3() {
  if (typeof Worker === "undefined" || typeof WebAssembly === "undefined") {
    return false;
  }
  if (typeof window !== "undefined" && !window.isSecureContext) {
    return false;
  }
  return true;
}
function callWorker(action, payload) {
  return new Promise((resolve, reject) => {
    if (!worker) {
      reject(new Error("Worker not initialized"));
      return;
    }
    const id = ++messageId;
    pendingMessages.set(id, { resolve, reject });
    worker.postMessage({ id, action, payload });
    setTimeout(() => {
      if (pendingMessages.has(id)) {
        pendingMessages.delete(id);
        reject(new Error(`Worker call timeout: ${action}`));
      }
    }, 3e4);
  });
}
async function initWorker() {
  if (worker) {
    return;
  }
  try {
    worker = new Worker(new URL("./sqliteWorker.js", import.meta.url), {
      type: "module"
    });
    worker.onmessage = (event) => {
      const { id, ok, result, error } = event.data;
      if (pendingMessages.has(id)) {
        const { resolve, reject } = pendingMessages.get(id);
        pendingMessages.delete(id);
        if (ok) {
          resolve(result);
        } else {
          reject(new Error(error || "Worker error"));
        }
      }
    };
    worker.onerror = (error) => {
      console.error("Worker error:", error);
    };
    await callWorker("init", {});
  } catch (error) {
    console.error("Failed to initialize worker:", error);
    throw error;
  }
}
async function create3(initialData, suggestedName = "pflanzenschutz.sqlite") {
  if (!isSupported3()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }
  await initWorker();
  await callWorker("importSnapshot", initialData);
  if (typeof window.showSaveFilePicker === "function") {
    try {
      fileHandle2 = await window.showSaveFilePicker({
        suggestedName,
        types: [
          {
            description: "SQLite Database",
            accept: { "application/x-sqlite3": [".sqlite", ".db"] }
          }
        ]
      });
      const exported = await callWorker("exportDB");
      const writable = await fileHandle2.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err2) {
      console.warn("Could not save SQLite file:", err2);
      fileHandle2 = null;
    }
  }
  return { data: initialData, context: { fileHandle: fileHandle2 } };
}
async function open3() {
  if (!isSupported3()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }
  await initWorker();
  if (typeof window.showOpenFilePicker === "function") {
    try {
      const [handle] = await window.showOpenFilePicker({
        types: [
          {
            description: "SQLite Database or JSON",
            accept: {
              "application/x-sqlite3": [".sqlite", ".db"],
              "application/json": [".json"]
            }
          }
        ]
      });
      fileHandle2 = handle;
      const file = await handle.getFile();
      const arrayBuffer = await file.arrayBuffer();
      if (file.name.endsWith(".json")) {
        const text = await file.text();
        const data = JSON.parse(text);
        await callWorker("importSnapshot", data);
        return { data, context: { fileHandle: fileHandle2 } };
      } else {
        await callWorker("importDB", arrayBuffer);
        const data = await callWorker("exportSnapshot");
        return { data, context: { fileHandle: fileHandle2 } };
      }
    } catch (err2) {
      console.error("Failed to open file:", err2);
      throw err2;
    }
  } else {
    throw new Error("File System Access API not available");
  }
}
async function save3(data) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importSnapshot", data);
  if (fileHandle2) {
    try {
      const exported = await callWorker("exportDB");
      const writable = await fileHandle2.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err2) {
      console.error("Failed to save to file:", err2);
    }
  }
  return { context: { fileHandle: fileHandle2 } };
}
function getContext3() {
  return { fileHandle: fileHandle2 };
}
function reset3() {
  if (worker) {
    worker.terminate();
    worker = null;
  }
  pendingMessages.clear();
  fileHandle2 = null;
  messageId = 0;
}
async function query(sql, params) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("query", { sql, params });
}
async function exec(sql) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("exec", { sql });
}
async function exportSnapshot() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportSnapshot");
}
async function importSnapshot(data) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importSnapshot", data);
}
async function exportDB() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportDB");
}
async function importDB(arrayBuffer) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importDB", arrayBuffer);
}
async function importBvlDataset(dataset) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlDataset", dataset);
}
async function importBvlSqlite(data, manifest) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlSqlite", { data, manifest });
}
async function getBvlMeta(key) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlMeta", key);
}
async function setBvlMeta(key, value) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("setBvlMeta", { key, value });
}
async function appendBvlSyncLog(entry) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("appendBvlSyncLog", entry);
}
async function listBvlSyncLog(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSyncLog", options);
}
async function queryZulassung(params) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryZulassung", params);
}
async function queryBvl(filters) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryBvl", filters);
}
async function getBvlSyncStatus() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlSyncStatus", {});
}
async function listBvlCultures(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlCultures", options);
}
async function listBvlSchadorg(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSchadorg", options);
}
async function diagnoseBvlSchema() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("diagnoseBvlSchema", {});
}

// src/scripts/core/storage/index.ts
var DRIVERS = {
  sqlite: sqlite_exports,
  filesystem: fileSystem_exports,
  localstorage: fallback_exports
};
function detectPreferredDriver() {
  if (isSupported3()) {
    return "sqlite";
  }
  if (isSupported()) {
    return "filesystem";
  }
  if (isSupported2()) {
    return "localstorage";
  }
  return "memory";
}
var activeDriverKey = detectPreferredDriver();
function setActiveDriver(driverKey) {
  if (driverKey !== "memory" && !DRIVERS[driverKey]) {
    throw new Error(`Unbekannter Storage-Treiber: ${driverKey}`);
  }
  activeDriverKey = driverKey;
  patchState({
    app: {
      ...getState().app,
      storageDriver: driverKey,
      hasFileAccess: driverKey === "filesystem"
    }
  });
}
function getActiveDriverKey() {
  return activeDriverKey;
}
function getDriver() {
  return DRIVERS[activeDriverKey] || null;
}
async function createDatabase(initialData) {
  const driver = getDriver();
  if (!driver) {
    throw new Error("Kein verf\xFCgbarer Speicher-Treiber");
  }
  return driver.create(initialData);
}
async function openDatabase() {
  const driver = getDriver();
  if (!driver) {
    throw new Error("Kein verf\xFCgbarer Speicher-Treiber");
  }
  return driver.open();
}
async function saveDatabase(data) {
  const driver = getDriver();
  if (!driver) {
    throw new Error("Kein verf\xFCgbarer Speicher-Treiber");
  }
  return driver.save(data);
}

// src/scripts/features/shared/mediumTable.ts
var COLUMN_FALLBACK_LABELS = {
  medium: "Mittel",
  unit: "Einheit",
  method: "Methode",
  value: "Wert",
  perQuantity: "Kisten",
  areaAr: "Ar",
  areaSqm: "m\xB2",
  total: "Gesamt"
};
var VARIANT_CONFIG = {
  calculation: {
    columns: [
      "medium",
      "unit",
      "method",
      "value",
      "perQuantity",
      "areaAr",
      "areaSqm",
      "total"
    ],
    numberFallback: "0.00",
    missingValue: "-"
  },
  detail: {
    columns: ["medium", "unit", "method", "value", "total"],
    numberFallback: "-",
    missingValue: "-"
  },
  summary: {
    columns: ["medium", "total"],
    numberFallback: "-",
    missingValue: "-"
  }
};
function resolveVariant(variant) {
  return VARIANT_CONFIG[variant] || VARIANT_CONFIG.calculation;
}
function resolveLabel(labels, key) {
  const label = labels?.calculation?.tableColumns?.[key];
  return escapeHtml(label || COLUMN_FALLBACK_LABELS[key] || key);
}
var COLUMN_DEFS = {
  medium: {
    cell: (item, config) => {
      const value = item?.name;
      return value ? escapeHtml(value) : config.missingValue;
    }
  },
  unit: {
    cell: (item, config) => {
      const value = item?.unit;
      return value ? escapeHtml(value) : config.missingValue;
    },
    className: "nowrap"
  },
  method: {
    cell: (item, config) => {
      const value = item?.methodLabel || item?.methodId;
      return value ? escapeHtml(value) : config.missingValue;
    }
  },
  value: {
    cell: (item, config) => formatNumber(item?.value, 2, config.numberFallback)
  },
  perQuantity: {
    cell: (item, config) => {
      const raw = item?.inputs?.kisten;
      if (raw === null || raw === void 0) {
        return config.missingValue;
      }
      return escapeHtml(String(raw));
    },
    className: "nowrap"
  },
  areaAr: {
    cell: (item, config) => formatNumber(item?.inputs?.areaAr, 2, config.numberFallback)
  },
  areaSqm: {
    cell: (item, config) => formatNumber(item?.inputs?.areaSqm, 2, config.numberFallback)
  },
  total: {
    cell: (item, config) => {
      const total = formatNumber(item?.total, 2, config.numberFallback);
      if (total === config.numberFallback) {
        return config.numberFallback;
      }
      const unit = item?.unit ? ` ${escapeHtml(item.unit)}` : "";
      return `${total}${unit}`;
    },
    className: "nowrap"
  }
};
function renderHeadCells(columns, labels) {
  return columns.map((columnKey) => `<th>${resolveLabel(labels, columnKey)}</th>`).join("");
}
function renderRowCells(item, columns, config) {
  return columns.map((columnKey) => {
    const column = COLUMN_DEFS[columnKey];
    if (!column) {
      return "<td>-</td>";
    }
    const value = column.cell(item, config);
    const className = column.className ? ` class="${column.className}"` : "";
    return `<td${className}>${value}</td>`;
  }).join("");
}
function buildMediumTableHead(labels, variant = "calculation") {
  const config = resolveVariant(variant);
  return `<tr>${renderHeadCells(config.columns, labels)}</tr>`;
}
function buildMediumTableRows(items = [], variant = "calculation") {
  const config = resolveVariant(variant);
  if (!Array.isArray(items) || !items.length) {
    return "";
  }
  return items.map((item) => `<tr>${renderRowCells(item, config.columns, config)}</tr>`).join("");
}
function buildMediumTableHTML(items = [], labels, variant = "calculation", options = {}) {
  const {
    classes = "table table-dark table-striped align-middle calc-medium-table"
  } = options;
  const head = buildMediumTableHead(labels, variant);
  const body = buildMediumTableRows(items, variant);
  return `<table class="${classes}"><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

// src/scripts/features/calculation/index.ts
var initialized = false;
function escapeAttr(value) {
  return escapeHtml(value);
}
function createSection(labels, defaultsState) {
  const formDefaults = defaultsState?.form || {
    creator: "",
    location: "",
    crop: "",
    quantity: ""
  };
  const section = document.createElement("section");
  section.className = "section-inner";
  section.innerHTML = `
    <div class="card card-dark mb-4 no-print">
      <div class="card-body">
        <form id="calculationForm" class="row g-3 no-print">
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.creator.label" data-default-label="${escapeAttr(labels.calculation.fields.creator.label)}" value="${escapeAttr(labels.calculation.fields.creator.label)}" placeholder="${escapeAttr(labels.calculation.fields.creator.label)}" aria-label="Bezeichnung f\xFCr Feld" />
            <input type="text" class="form-control" id="calc-ersteller" name="calc-ersteller" required data-placeholder-id="calc-form-creator" placeholder="${escapeAttr(labels.calculation.fields.creator.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.creator.label)}" value="${escapeAttr(formDefaults.creator || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.location.label" data-default-label="${escapeAttr(labels.calculation.fields.location.label)}" value="${escapeAttr(labels.calculation.fields.location.label)}" placeholder="${escapeAttr(labels.calculation.fields.location.label)}" aria-label="Bezeichnung f\xFCr Feld" />
            <input type="text" class="form-control" id="calc-standort" name="calc-standort" data-placeholder-id="calc-form-location" placeholder="${escapeAttr(labels.calculation.fields.location.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.location.label)}" value="${escapeAttr(formDefaults.location || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.crop.label" data-default-label="${escapeAttr(labels.calculation.fields.crop.label)}" value="${escapeAttr(labels.calculation.fields.crop.label)}" placeholder="${escapeAttr(labels.calculation.fields.crop.label)}" aria-label="Bezeichnung f\xFCr Feld" />
            <input type="text" class="form-control" id="calc-kultur" name="calc-kultur" data-placeholder-id="calc-form-crop" placeholder="${escapeAttr(labels.calculation.fields.crop.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.crop.label)}" value="${escapeAttr(formDefaults.crop || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.quantity.label" data-default-label="${escapeAttr(labels.calculation.fields.quantity.label)}" value="${escapeAttr(labels.calculation.fields.quantity.label)}" placeholder="${escapeAttr(labels.calculation.fields.quantity.label)}" aria-label="Bezeichnung f\xFCr Feld" />
            <input type="number" min="0" step="any" class="form-control" id="calc-kisten" name="calc-kisten" required data-placeholder-id="calc-form-quantity" placeholder="${escapeAttr(labels.calculation.fields.quantity.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.quantity.label)}" value="${escapeAttr(formDefaults.quantity || "")}" />
          </div>
          <div class="col-12 text-center">
            <button type="submit" class="btn btn-success px-4">Berechnen</button>
          </div>
        </form>
      </div>
    </div>
    <div id="calc-result" class="card card-dark d-none">
      <div class="card-body">
        <div class="calc-summary mb-3">
          <div class="calc-summary-columns">
            <div class="calc-summary-column calc-summary-main">
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-creator">${escapeHtml(labels.calculation.fields.creator.label)}</span>
                <span class="calc-summary-value" data-field="ersteller"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-location">${escapeHtml(labels.calculation.fields.location.label)}</span>
                <span class="calc-summary-value" data-field="standort"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-crop">${escapeHtml(labels.calculation.fields.crop.label)}</span>
                <span class="calc-summary-value" data-field="kultur"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-date">${escapeHtml(labels.calculation.summary.dateLabel || "Datum")}</span>
                <span class="calc-summary-value" data-field="datum"></span>
              </div>
            </div>
            <div class="calc-summary-column calc-summary-company">
              <div class="calc-summary-row d-none" data-company-row="headline">
                <span class="calc-summary-label">Claim</span>
                <span class="calc-summary-value" data-field="company-headline"></span>
              </div>
              <div class="calc-summary-row d-none" data-company-row="address">
                <span class="calc-summary-label">Anschrift</span>
                <span class="calc-summary-value calc-summary-value--multiline" data-field="company-address"></span>
              </div>
              <div class="calc-summary-row d-none" data-company-row="email">
                <span class="calc-summary-label">E-Mail</span>
                <a class="calc-summary-value" data-field="company-email"></a>
              </div>
            </div>
          </div>
        </div>
        <div class="calc-table-wrapper">
          <table class="table table-dark table-striped align-middle calc-medium-table" id="calc-results-table">
            <thead></thead>
            <tbody></tbody>
          </table>
        </div>
        <div class="mt-3 no-print">
          <button class="btn btn-outline-secondary" data-action="print">Drucken / PDF</button>
          <button class="btn btn-primary ms-2" data-action="save">Aktuelle Berechnung speichern</button>
        </div>
      </div>
    </div>
  `;
  return section;
}
function applyFieldLabels(section, labels) {
  const labelMap = {
    "calc-form-creator": labels.calculation.fields.creator.label,
    "calc-form-location": labels.calculation.fields.location.label,
    "calc-form-crop": labels.calculation.fields.crop.label,
    "calc-form-quantity": labels.calculation.fields.quantity.label,
    "calc-summary-creator": labels.calculation.fields.creator.label,
    "calc-summary-location": labels.calculation.fields.location.label,
    "calc-summary-crop": labels.calculation.fields.crop.label,
    "calc-summary-date": labels.calculation.summary.dateLabel || "Datum"
  };
  Object.entries(labelMap).forEach(([key, text]) => {
    const element = section.querySelector(`[data-label-id="${key}"]`);
    if (element) {
      element.textContent = typeof text === "string" ? text : "";
    }
  });
  section.querySelectorAll(".label-editor").forEach((input) => {
    const path = input.dataset.labelEditor;
    if (!path) {
      return;
    }
    const value = path.split(".").reduce(
      (acc, segment) => acc && acc[segment] !== void 0 ? acc[segment] : null,
      labels
    );
    if (typeof value === "string") {
      input.placeholder = value;
      input.dataset.defaultLabel = value;
      if (!input.matches(":focus")) {
        input.value = value;
      }
    }
  });
  const placeholderMap = {
    "calc-form-creator": labels.calculation.fields.creator.placeholder,
    "calc-form-location": labels.calculation.fields.location.placeholder,
    "calc-form-crop": labels.calculation.fields.crop.placeholder,
    "calc-form-quantity": labels.calculation.fields.quantity.placeholder
  };
  Object.entries(placeholderMap).forEach(([key, text]) => {
    const element = section.querySelector(
      `[data-placeholder-id="${key}"]`
    );
    if (element) {
      element.setAttribute("placeholder", typeof text === "string" ? text : "");
    }
  });
  const tableHead = section.querySelector("#calc-results-table thead");
  if (tableHead) {
    tableHead.innerHTML = buildMediumTableHead(labels, "calculation");
  }
}
async function persistHistory(services2) {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === "memory") {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    services2.events.emit("database:saved", {
      scope: "history",
      driver: driverKey
    });
  } catch (err2) {
    console.error("Automatisches Speichern der Historie fehlgeschlagen", err2);
    services2.events.emit("database:error", { scope: "history", error: err2 });
    window.alert(
      "Berechnung gespeichert, aber die Datei konnte nicht aktualisiert werden. Bitte manuell sichern."
    );
  }
}
function getWaterVolume(kisten, defaults) {
  const arValue = kisten / (defaults.kistenProAr || 1);
  return arValue * (defaults.waterPerKisteL || 0);
}
function executeFormula(medium, method, inputs) {
  if (!method) {
    return 0;
  }
  const value = Number(medium.value) || 0;
  switch (method.type) {
    case "factor": {
      const base = inputs[method.config?.sourceField || "kisten"] || 0;
      return value * base;
    }
    case "percentOf": {
      const base = inputs[method.config?.baseField || "waterVolume"] || 0;
      return base * value / 100;
    }
    case "fixed":
      return value;
    default:
      console.warn("Unbekannter Methodentyp", method.type);
      return value;
  }
}
function renderResults(section, calculation, labels) {
  const resultCard = section.querySelector("#calc-result");
  if (!resultCard) {
    return;
  }
  const resultsTable = resultCard.querySelector(
    "#calc-results-table"
  );
  const resultsHead = resultsTable?.querySelector("thead");
  const resultsBody = resultsTable?.querySelector("tbody");
  const stateSnapshot = getState();
  const companyData = stateSnapshot.company || {};
  if (!calculation) {
    resultCard.classList.add("d-none");
    if (resultsBody) {
      resultsBody.innerHTML = "";
    }
    return;
  }
  const { header, items } = calculation;
  const setFieldText = (key, value) => {
    const el = resultCard.querySelector(`[data-field="${key}"]`);
    if (el) {
      el.textContent = value ?? "";
    }
  };
  setFieldText("ersteller", header.ersteller);
  setFieldText("standort", header.standort);
  setFieldText("kultur", header.kultur);
  setFieldText("datum", header.datum);
  const updateCompanyRowVisibility = (rowKey, visible) => {
    const row = resultCard.querySelector(
      `[data-company-row="${rowKey}"]`
    );
    if (row) {
      row.classList.toggle("d-none", !visible);
    }
  };
  const headlineValue = companyData.headline?.trim() || "";
  setFieldText("company-headline", headlineValue);
  updateCompanyRowVisibility("headline", Boolean(headlineValue));
  const addressValue = companyData.address?.trim() || "";
  const addressEl = resultCard.querySelector(
    '[data-field="company-address"]'
  );
  if (addressEl) {
    addressEl.textContent = addressValue;
  }
  updateCompanyRowVisibility("address", Boolean(addressValue));
  const emailEl = resultCard.querySelector(
    '[data-field="company-email"]'
  );
  const emailValue = companyData.contactEmail?.trim() || "";
  if (emailEl) {
    if (emailValue) {
      emailEl.textContent = emailValue;
      emailEl.setAttribute("href", `mailto:${emailValue}`);
    } else {
      emailEl.textContent = "";
      emailEl.removeAttribute("href");
    }
  }
  updateCompanyRowVisibility("email", Boolean(emailValue));
  const companyColumn = resultCard.querySelector(
    ".calc-summary-company"
  );
  if (companyColumn) {
    const hasVisibleRow = Array.from(
      companyColumn.querySelectorAll("[data-company-row]")
    ).some((row) => !row.classList.contains("d-none"));
    companyColumn.classList.toggle("d-none", !hasVisibleRow);
  }
  if (resultsHead) {
    resultsHead.innerHTML = buildMediumTableHead(labels, "calculation");
  }
  if (resultsBody) {
    resultsBody.innerHTML = buildMediumTableRows(items, "calculation");
  }
  resultCard.classList.remove("d-none");
}
function initCalculation(container2, services2) {
  if (!container2 || initialized) {
    return;
  }
  const host = container2;
  const initialState = services2.state.getState();
  host.innerHTML = "";
  const section = createSection(
    initialState.fieldLabels,
    initialState.defaults
  );
  host.appendChild(section);
  applyFieldLabels(section, initialState.fieldLabels);
  const form = section.querySelector("#calculationForm");
  const resultCard = section.querySelector("#calc-result");
  const resultsTable = section.querySelector(
    "#calc-results-table"
  );
  const resultsHead = resultsTable?.querySelector("thead");
  const resultsBody = resultsTable?.querySelector("tbody");
  if (!form || !resultCard || !resultsTable || !resultsHead || !resultsBody) {
    console.warn("Berechnungsbereich konnte nicht initialisiert werden");
    return;
  }
  const setCalcContext = (calculation) => {
    renderResults(section, calculation, services2.state.getState().fieldLabels);
  };
  services2.state.subscribe((nextState) => {
    applyFieldLabels(section, nextState.fieldLabels);
    setCalcContext(nextState.calcContext);
  });
  section.querySelectorAll(".label-editor").forEach((input) => {
    input.addEventListener("change", (event) => {
      const target = event.currentTarget;
      const path = target.dataset.labelEditor;
      if (!path) {
        return;
      }
      const trimmed = target.value.trim();
      const fallback = target.dataset.defaultLabel || target.getAttribute("placeholder") || target.value;
      const nextValue = trimmed || fallback || "";
      if (!trimmed) {
        target.value = nextValue;
      }
      services2.state.updateSlice(
        "fieldLabels",
        (currentLabels) => setFieldLabelByPath(currentLabels, path, nextValue)
      );
    });
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const rawErsteller = (formData.get("calc-ersteller") || "").toString().trim();
    const rawStandort = (formData.get("calc-standort") || "").toString().trim();
    const rawKultur = (formData.get("calc-kultur") || "").toString().trim();
    const rawKisten = (formData.get("calc-kisten") || "").toString().trim();
    const ersteller = rawErsteller;
    const standort = rawStandort || "-";
    const kultur = rawKultur || "-";
    const kisten = Number(rawKisten);
    if (!ersteller || Number.isNaN(kisten)) {
      window.alert("Bitte Felder korrekt ausf\xFCllen!");
      return;
    }
    const state2 = services2.state.getState();
    const defaults = state2.defaults;
    const measurementMethods = state2.measurementMethods;
    const waterVolume = getWaterVolume(kisten, defaults);
    const areaAr = defaults.kistenProAr ? kisten / defaults.kistenProAr : 0;
    const areaSqm = areaAr * 100;
    const inputs = {
      kisten,
      waterVolume,
      areaAr,
      areaSqm
    };
    const items = state2.mediums.map((medium) => {
      const method = measurementMethods.find(
        (m) => m.id === medium.methodId
      );
      const total = executeFormula(medium, method, inputs);
      return {
        id: medium.id,
        name: medium.name,
        unit: medium.unit,
        methodLabel: method ? method.label : medium.methodId,
        methodId: medium.methodId,
        value: medium.value,
        total,
        inputs
      };
    });
    const header = {
      ersteller,
      standort,
      kultur,
      kisten,
      datum: (/* @__PURE__ */ new Date()).toLocaleDateString("de-DE", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
      }),
      waterVolume,
      areaAr,
      areaSqm
    };
    const calculation = {
      header,
      items
    };
    services2.state.updateSlice("defaults", (defaultsState) => ({
      ...defaultsState,
      form: {
        ...defaultsState.form || {
          creator: "",
          location: "",
          crop: "",
          quantity: ""
        },
        creator: rawErsteller,
        location: rawStandort,
        crop: rawKultur,
        quantity: rawKisten
      }
    }));
    services2.state.updateSlice("calcContext", () => calculation);
  });
  resultCard.addEventListener("click", (event) => {
    const target = event.target;
    const action = target?.dataset?.action;
    if (!action) {
      return;
    }
    if (action === "print") {
      window.print();
    } else if (action === "save") {
      const calc = services2.state.getState().calcContext;
      if (!calc) {
        window.alert("Keine Berechnung vorhanden.");
        return;
      }
      services2.state.updateSlice("history", (history) => {
        const entry = {
          ...calc.header,
          items: calc.items,
          savedAt: (/* @__PURE__ */ new Date()).toISOString()
        };
        return [...history, entry];
      });
      void persistHistory(services2).catch((err2) => {
        console.error("Persist history promise error", err2);
      });
      window.alert("Berechnung gespeichert! (Siehe Historie)");
    }
  });
  setCalcContext(initialState.calcContext);
  initialized = true;
}

// src/scripts/features/startup/index.ts
var DOWNLOAD_FILENAME_FALLBACK = "pflanzenschutz-datenbank.json";
var initialized2 = false;
function sanitizeFilename(name) {
  if (!name) {
    return DOWNLOAD_FILENAME_FALLBACK;
  }
  const slug = name.toString().toLowerCase().trim().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  return `${slug || "pflanzenschutz-datenbank"}.json`;
}
async function withButtonBusy(button, task) {
  if (!button) {
    await task();
    return;
  }
  const originalText = button.textContent ?? "";
  button.disabled = true;
  button.dataset.busy = "true";
  button.textContent = "Bitte warten...";
  try {
    await task();
  } finally {
    button.disabled = false;
    button.dataset.busy = "false";
    button.textContent = originalText;
  }
}
function escapeAttr2(value) {
  return escapeHtml(value);
}
function createWizard(baseCompany) {
  const section = document.createElement("section");
  section.className = "section-container d-none";
  section.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body">
          <div class="d-flex justify-content-between align-items-start flex-wrap gap-3 mb-3">
            <div>
              <h2 class="mb-1">Neue Datenbank konfigurieren</h2>
              <p class="text-muted mb-0">Erfasse deine Eckdaten. Daraus entsteht eine JSON-Datei f\xFCr den sp\xE4teren Import.</p>
            </div>
            <button type="button" class="btn btn-outline-light" data-action="wizard-back">Zur\xFCck</button>
          </div>
          <form id="database-wizard-form" class="row g-3 text-start">
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-name">Firmenname*</label>
              <input class="form-control" id="wizard-company-name" name="wizard-company-name" required value="${escapeAttr2(baseCompany.name)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-headline">\xDCberschrift / Claim</label>
              <input class="form-control" id="wizard-company-headline" name="wizard-company-headline" value="${escapeAttr2(baseCompany.headline)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-logo">Logo-URL</label>
              <input class="form-control" id="wizard-company-logo" name="wizard-company-logo" placeholder="https://example.com/logo.png" value="${escapeAttr2(baseCompany.logoUrl)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-email">Kontakt-E-Mail</label>
              <input class="form-control" id="wizard-company-email" name="wizard-company-email" value="${escapeAttr2(baseCompany.contactEmail)}" />
            </div>
            <div class="col-12">
              <label class="form-label" for="wizard-company-address">Adresse</label>
              <textarea class="form-control" id="wizard-company-address" name="wizard-company-address" rows="2">${escapeHtml(baseCompany.address)}</textarea>
            </div>
            <div class="col-12 d-flex flex-column flex-md-row gap-3">
              <button type="submit" class="btn btn-success px-4">Datenbank erzeugen</button>
              <button type="button" class="btn btn-outline-light px-4" data-action="wizard-back">Abbrechen</button>
            </div>
          </form>
        </div>
      </div>
      <div class="card card-dark mt-4 d-none" data-role="wizard-result">
        <div class="card-body">
          <h3 class="h5 mb-3">Datenbank erstellt</h3>
          <p class="mb-2">Vorschlag f\xFCr den Dateinamen: <code data-role="wizard-filename"></code></p>
          <div class="d-flex flex-column flex-lg-row gap-2 mb-3">
            <button type="button" class="btn btn-success" data-action="wizard-save">Datei speichern</button>
          </div>
          <p class="text-muted small mb-2" data-role="wizard-save-hint"></p>
          <p class="text-muted small mb-2">Vorschau der kompletten JSON-Struktur (scrollbar):</p>
          <pre class="bg-dark rounded p-3 small overflow-auto" style="max-height: 14rem;" data-role="wizard-preview"></pre>
        </div>
      </div>
    </div>
  `;
  const form = section.querySelector("#database-wizard-form");
  if (!form) {
    throw new Error("Wizard-Formular konnte nicht erzeugt werden");
  }
  const resultCard = section.querySelector(
    '[data-role="wizard-result"]'
  );
  if (!resultCard) {
    throw new Error("Wizard-Resultat-Container fehlt");
  }
  return {
    section,
    form,
    resultCard,
    preview: section.querySelector(
      '[data-role="wizard-preview"]'
    ),
    filenameLabel: section.querySelector(
      '[data-role="wizard-filename"]'
    ),
    saveHint: section.querySelector(
      '[data-role="wizard-save-hint"]'
    ),
    saveButton: section.querySelector(
      '[data-action="wizard-save"]'
    ),
    reset() {
      form.reset();
      resultCard.classList.add("d-none");
      const preview = section.querySelector(
        '[data-role="wizard-preview"]'
      );
      if (preview) {
        preview.textContent = "";
      }
      const filenameLabel = section.querySelector(
        '[data-role="wizard-filename"]'
      );
      if (filenameLabel) {
        filenameLabel.textContent = "";
      }
    }
  };
}
function initStartup(container2, services2) {
  if (!container2 || initialized2) {
    return;
  }
  const host = container2;
  let generatedDatabase = null;
  let generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
  let activeView = "landing";
  const stateSnapshot = services2.state.getState();
  const baseCompany = stateSnapshot.company;
  const landingSection = document.createElement("section");
  landingSection.className = "section-container";
  const activeDriver = getActiveDriverKey();
  const usingSQLite = activeDriver === "sqlite";
  const storageInfo = usingSQLite ? "" : "Die erzeugte JSON-Datei kann sp\xE4ter erneut geladen oder weitergegeben werden.";
  landingSection.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body text-center">
          <h2 class="mb-3">Datenbank starten</h2>
          <p class="mb-4">
            Verwalte und berechne Pflanzenschutzmittel zentral: Lege eine neue SQLite-Datenbank an oder verbinde eine vorhandene Datei, die du lokal speichern und jederzeit erneut verwenden kannst.
          </p>
          <div class="d-flex flex-column flex-md-row gap-3 justify-content-center">
            <button class="btn btn-success px-4" data-action="start-wizard">Neue Datenbank erstellen</button>
            <button class="btn btn-outline-light px-4" data-action="open">Bestehende Datei verbinden</button>
            <button class="btn btn-secondary px-4" data-action="useDefaults">Defaults testen</button>
          </div>
          ${storageInfo ? `<p class="mt-3 text-muted mb-0 small">${storageInfo}</p>` : ""}
        </div>
      </div>
    </div>
  `;
  const wizard = createWizard(baseCompany);
  host.innerHTML = "";
  host.appendChild(landingSection);
  host.appendChild(wizard.section);
  const fileSystemSupported = typeof window !== "undefined" && typeof window.showSaveFilePicker === "function";
  if (wizard.saveButton) {
    if (!fileSystemSupported) {
      wizard.saveButton.disabled = true;
      wizard.saveButton.textContent = "Datei speichern (nicht verf\xFCgbar)";
      if (wizard.saveHint) {
        wizard.saveHint.textContent = "Dieser Browser unterst\xFCtzt keinen direkten Dateidialog. Bitte nutze einen Chromium-basierten Browser (z. B. Chrome, Edge) \xFCber HTTPS oder http://localhost.";
      }
    } else if (wizard.saveHint) {
      wizard.saveHint.textContent = 'Der Browser fragt nach einem Speicherort. Die erzeugte Datei kannst du sp\xE4ter \xFCber "Bestehende Datei verbinden" erneut laden.';
    }
  }
  function updateRegionVisibility(state2 = services2.state.getState()) {
    const hasDatabase = Boolean(state2.app?.hasDatabase);
    host.classList.toggle("d-none", hasDatabase);
    if (hasDatabase) {
      landingSection.classList.add("d-none");
      wizard.section.classList.add("d-none");
      return;
    }
    if (activeView === "wizard") {
      landingSection.classList.add("d-none");
      wizard.section.classList.remove("d-none");
    } else {
      landingSection.classList.remove("d-none");
      wizard.section.classList.add("d-none");
    }
  }
  async function handleOpen(button) {
    await withButtonBusy(button, async () => {
      try {
        const preferred = getActiveDriverKey();
        if (preferred === "sqlite" || preferred === "filesystem") {
          setActiveDriver(preferred);
        } else {
          setActiveDriver("filesystem");
        }
      } catch (err2) {
        window.alert(
          "Dateisystemzugriff wird nicht unterst\xFCtzt in diesem Browser."
        );
        throw err2 instanceof Error ? err2 : new Error("Dateisystem nicht verf\xFCgbar");
      }
      try {
        const result = await openDatabase();
        applyDatabase(result.data);
        services2.events.emit("database:connected", {
          driver: getActiveDriverKey()
        });
      } catch (err2) {
        console.error("Fehler beim \xD6ffnen der Datenbank", err2);
        window.alert(
          err2 instanceof Error ? err2.message : "\xD6ffnen der Datenbank fehlgeschlagen"
        );
      }
    });
  }
  function handleUseDefaults(button) {
    void withButtonBusy(button, async () => {
      try {
        try {
          setActiveDriver("localstorage");
        } catch (err2) {
          console.warn(
            "LocalStorage-Treiber nicht verf\xFCgbar, nutze Memory",
            err2
          );
          setActiveDriver("memory");
        }
        const initialData = createInitialDatabase();
        applyDatabase(initialData);
        services2.events.emit("database:connected", {
          driver: getActiveDriverKey() || "memory"
        });
      } catch (err2) {
        console.error("Fehler beim Laden der Defaults", err2);
        window.alert(
          err2 instanceof Error ? err2.message : "Defaults konnten nicht geladen werden"
        );
      }
    });
  }
  async function handleWizardSave(button) {
    if (!generatedDatabase) {
      window.alert("Bitte erst die Datenbank erzeugen.");
      return;
    }
    await withButtonBusy(button, async () => {
      try {
        const preferred = getActiveDriverKey();
        if (preferred === "sqlite" || preferred === "filesystem") {
          setActiveDriver(preferred);
        } else {
          setActiveDriver("filesystem");
        }
      } catch (err2) {
        window.alert(
          "Dateisystemzugriff wird nicht unterst\xFCtzt in diesem Browser."
        );
        throw err2 instanceof Error ? err2 : new Error("Dateisystem nicht verf\xFCgbar");
      }
      try {
        const result = await createDatabase(generatedDatabase);
        applyDatabase(result.data);
        services2.events.emit("database:connected", {
          driver: getActiveDriverKey()
        });
      } catch (err2) {
        console.error("Fehler beim Speichern der Datenbank", err2);
        window.alert(
          err2 instanceof Error ? err2.message : "Die Datei konnte nicht gespeichert werden"
        );
      }
    });
  }
  function handleWizardSubmit(event) {
    event.preventDefault();
    const formData = new FormData(wizard.form);
    const name = (formData.get("wizard-company-name") || "").toString().trim();
    if (!name) {
      window.alert("Bitte einen Firmennamen angeben.");
      return;
    }
    const headline = (formData.get("wizard-company-headline") || "").toString().trim();
    const logoUrl = (formData.get("wizard-company-logo") || "").toString().trim();
    const contactEmail = (formData.get("wizard-company-email") || "").toString().trim();
    const address = (formData.get("wizard-company-address") || "").toString().trim();
    const overrides = {
      meta: {
        company: {
          name,
          headline,
          logoUrl,
          contactEmail,
          address
        }
      }
    };
    generatedDatabase = createInitialDatabase(overrides);
    generatedFilename = sanitizeFilename(name);
    wizard.preview.textContent = JSON.stringify(generatedDatabase, null, 2);
    wizard.filenameLabel.textContent = generatedFilename;
    wizard.resultCard.classList.remove("d-none");
    wizard.resultCard.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  function showLanding() {
    activeView = "landing";
    generatedDatabase = null;
    generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
    wizard.reset();
    updateRegionVisibility();
  }
  function showWizard() {
    activeView = "wizard";
    updateRegionVisibility();
  }
  landingSection.addEventListener("click", (event) => {
    const button = event.target?.closest(
      "button[data-action]"
    );
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === "start-wizard") {
      showWizard();
      return;
    }
    if (action === "open") {
      void handleOpen(button);
    } else if (action === "useDefaults") {
      handleUseDefaults(button);
    }
  });
  wizard.form.addEventListener("submit", handleWizardSubmit);
  wizard.section.addEventListener("click", (event) => {
    const button = event.target?.closest(
      "[data-action]"
    );
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === "wizard-back") {
      showLanding();
      return;
    }
    if (action === "wizard-save") {
      void handleWizardSave(button);
    }
  });
  services2.state.subscribe((nextState) => updateRegionVisibility(nextState));
  updateRegionVisibility(services2.state.getState());
  if (!services2.state.getState().app.hasDatabase) {
    const preferred = detectPreferredDriver();
    if (preferred && preferred !== getActiveDriverKey()) {
      try {
        setActiveDriver(preferred);
      } catch (err2) {
        console.warn("Bevorzugter Speicher konnte nicht gesetzt werden", err2);
      }
    }
  }
  initialized2 = true;
}

// src/scripts/features/shared/calculationSnapshot.ts
function renderCalculationSnapshot(entry, labels, options = {}) {
  const {
    showActions = false,
    includeCheckbox = false,
    index,
    selected = false
  } = options;
  const tableLabels = labels?.history?.tableColumns ?? {};
  const detailLabels = labels?.history?.detail ?? {};
  const selectedClass = selected ? " calc-snapshot-card--selected" : "";
  const dataIndex = typeof index === "number" ? ` data-index="${index}"` : "";
  const checkboxHtml = includeCheckbox && typeof index === "number" ? `<div class="calc-snapshot-card__checkbox no-print">
           <input type="checkbox"
                  class="form-check-input"
                  data-action="toggle-select"
                  data-index="${index}"
                  ${selected ? "checked" : ""}
                  aria-label="Eintrag ausw\xE4hlen" />
         </div>` : "";
  const actionsHtml = showActions && typeof index === "number" ? `<div class="calc-snapshot-card__actions no-print">
           <button class="btn btn-sm btn-info"
                   data-action="view"
                   data-index="${index}">
             Ansehen
           </button>
           <button class="btn btn-sm btn-danger"
                   data-action="delete"
                   data-index="${index}">
             L\xF6schen
           </button>
         </div>` : "";
  const mediumTable = buildMediumTableHTML(
    entry.items || [],
    labels,
    "summary",
    {
      classes: "calc-snapshot-table"
    }
  );
  return `
    <div class="calc-snapshot-card${selectedClass}"${dataIndex}>
      ${checkboxHtml}
      <div class="calc-snapshot-card__header">
        <div class="calc-snapshot-card__meta">
          <div class="calc-snapshot-card__date">
            <strong>${escapeHtml(tableLabels.date || "Datum")}:</strong>
            ${escapeHtml(entry?.datum || entry?.date || "\u2013")}
          </div>
          <div class="calc-snapshot-card__creator">
            <strong>${escapeHtml(detailLabels.creator || tableLabels.creator || "Erstellt von")}:</strong>
            ${escapeHtml(entry?.ersteller || "\u2013")}
          </div>
        </div>
      </div>
      <div class="calc-snapshot-card__body">
        <div class="calc-snapshot-card__info">
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.location || tableLabels.location || "Standort")}:</strong>
            ${escapeHtml(entry?.standort || "\u2013")}
          </div>
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.crop || tableLabels.crop || "Kultur")}:</strong>
            ${escapeHtml(entry?.kultur || "\u2013")}
          </div>
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.quantity || tableLabels.quantity || "Kisten")}:</strong>
            ${escapeHtml(
    entry?.kisten !== void 0 && entry?.kisten !== null ? String(entry.kisten) : "\u2013"
  )}
          </div>
        </div>
        <div class="calc-snapshot-card__mediums">
          ${mediumTable}
        </div>
      </div>
      ${actionsHtml}
    </div>
  `;
}
function renderCalculationSnapshotForPrint(entry, labels) {
  const detailLabels = labels?.history?.detail ?? {};
  const mediumTable = buildMediumTableHTML(
    entry.items || [],
    labels,
    "detail",
    {
      classes: "history-detail-table"
    }
  );
  return `
    <div class="calc-snapshot-print">
      <div class="calc-snapshot-print__header">
        <h3>${escapeHtml(detailLabels.title || "Details")} \u2013 ${escapeHtml(entry?.datum || entry?.date || "")}</h3>
      </div>
      <div class="calc-snapshot-print__meta">
        <p>
          <strong>${escapeHtml(detailLabels.creator || "Erstellt von")}:</strong>
          ${escapeHtml(entry?.ersteller || "\u2013")}<br />
          <strong>${escapeHtml(detailLabels.location || "Standort")}:</strong>
          ${escapeHtml(entry?.standort || "\u2013")}<br />
          <strong>${escapeHtml(detailLabels.crop || "Kultur")}:</strong>
          ${escapeHtml(entry?.kultur || "\u2013")}<br />
          <strong>${escapeHtml(detailLabels.quantity || "Kisten")}:</strong>
          ${escapeHtml(
    entry?.kisten !== void 0 && entry?.kisten !== null ? String(entry.kisten) : "\u2013"
  )}
        </p>
      </div>
      <div class="calc-snapshot-print__mediums">
        ${mediumTable}
      </div>
    </div>
  `;
}

// src/scripts/core/virtualList.ts
function initVirtualList(container2, {
  itemCount,
  estimatedItemHeight,
  renderItem,
  overscan = 6,
  onRangeChange
}) {
  if (!container2 || typeof renderItem !== "function") {
    throw new Error("initVirtualList requires a container and renderItem function");
  }
  let currentItemCount = itemCount || 0;
  let currentEstimatedHeight = estimatedItemHeight || 100;
  let visibleStart = 0;
  let visibleEnd = 0;
  let isDestroyed = false;
  container2.style.overflow = "auto";
  container2.style.position = "relative";
  const spacer = document.createElement("div");
  spacer.style.position = "absolute";
  spacer.style.top = "0";
  spacer.style.left = "0";
  spacer.style.width = "1px";
  spacer.style.height = `${currentItemCount * currentEstimatedHeight}px`;
  spacer.style.pointerEvents = "none";
  container2.appendChild(spacer);
  const itemsContainer = document.createElement("div");
  itemsContainer.style.position = "relative";
  itemsContainer.style.width = "100%";
  container2.appendChild(itemsContainer);
  const nodePool = [];
  const maxPoolSize = Math.ceil(container2.clientHeight / currentEstimatedHeight) + overscan * 2 + 5;
  function getNode() {
    if (nodePool.length > 0) {
      return nodePool.pop();
    }
    const node = document.createElement("div");
    node.style.position = "absolute";
    node.style.top = "0";
    node.style.left = "0";
    node.style.width = "100%";
    return node;
  }
  function releaseNode(node) {
    if (nodePool.length < maxPoolSize * 2) {
      node.innerHTML = "";
      node.removeAttribute("data-index");
      node.className = "";
      nodePool.push(node);
    } else {
      node.remove();
    }
  }
  function calculateVisibleRange() {
    const scrollTop = container2.scrollTop;
    const viewportHeight = container2.clientHeight;
    const startIndex = Math.floor(scrollTop / currentEstimatedHeight);
    const endIndex = Math.ceil((scrollTop + viewportHeight) / currentEstimatedHeight);
    const start = Math.max(0, startIndex - overscan);
    const end = Math.min(currentItemCount, endIndex + overscan);
    return { start, end };
  }
  function render2() {
    if (isDestroyed) return;
    const { start, end } = calculateVisibleRange();
    if (start === visibleStart && end === visibleEnd) {
      return;
    }
    visibleStart = start;
    visibleEnd = end;
    const existingNodes = Array.from(itemsContainer.children);
    const nodesToKeep = /* @__PURE__ */ new Map();
    const nodesToRelease = [];
    existingNodes.forEach((node) => {
      const index = parseInt(node.dataset.index, 10);
      if (index >= start && index < end) {
        nodesToKeep.set(index, node);
      } else {
        nodesToRelease.push(node);
      }
    });
    nodesToRelease.forEach((node) => {
      releaseNode(node);
      itemsContainer.removeChild(node);
    });
    for (let i = start; i < end; i++) {
      let node = nodesToKeep.get(i);
      if (!node) {
        node = getNode();
        node.dataset.index = String(i);
        itemsContainer.appendChild(node);
      }
      node.style.transform = `translateY(${i * currentEstimatedHeight}px)`;
      renderItem(node, i);
    }
    if (onRangeChange) {
      onRangeChange(start, end);
    }
  }
  function updateSpacerHeight() {
    spacer.style.height = `${currentItemCount * currentEstimatedHeight}px`;
  }
  let scrollTimeout = null;
  function handleScroll() {
    if (scrollTimeout) {
      clearTimeout(scrollTimeout);
    }
    scrollTimeout = setTimeout(() => {
      render2();
    }, 16);
  }
  container2.addEventListener("scroll", handleScroll);
  render2();
  return {
    /**
     * Updates the item count and re-renders
     */
    updateItemCount(newCount) {
      currentItemCount = newCount || 0;
      updateSpacerHeight();
      render2();
    },
    /**
     * Scrolls to show a specific item
     */
    scrollToIndex(index) {
      if (index < 0 || index >= currentItemCount) return;
      container2.scrollTop = index * currentEstimatedHeight;
      render2();
    },
    /**
     * Cleans up and removes all event listeners
     */
    destroy() {
      if (isDestroyed) return;
      isDestroyed = true;
      container2.removeEventListener("scroll", handleScroll);
      if (scrollTimeout) {
        clearTimeout(scrollTimeout);
      }
      Array.from(itemsContainer.children).forEach((node) => {
        node.remove();
      });
      spacer.remove();
      itemsContainer.remove();
      nodePool.length = 0;
    }
  };
}

// src/scripts/core/print.ts
var BASE_FONT_STACK = '"Helvetica Neue", Helvetica, Arial, sans-serif';
var overlayElement = null;
function ensureOverlay() {
  if (!overlayElement) {
    overlayElement = document.createElement("div");
    overlayElement.id = "print-overlay";
    overlayElement.className = "print-overlay";
    document.body.appendChild(overlayElement);
  }
  return overlayElement;
}
function cleanupOverlay() {
  if (!overlayElement) {
    return;
  }
  overlayElement.innerHTML = "";
  document.body.classList.remove("print-overlay-active");
}
function openPopup(html, onFail) {
  let popup = null;
  try {
    popup = window.open("", "_blank", "noopener,noreferrer,width=1024,height=768");
  } catch (err2) {
    console.warn("window.open failed", err2);
  }
  if (!popup) {
    if (onFail) {
      onFail();
    }
    return null;
  }
  try {
    popup.document.open();
    popup.document.write(html);
    popup.document.close();
  } catch (err2) {
    console.error("Unable to write into print window", err2);
    if (onFail) {
      onFail(popup);
    }
    return null;
  }
  return popup;
}
function printHtml({ title = "Druck", styles = "", content = "" }) {
  const safeTitle = String(title ?? "Druck").replace(/[<>]/g, "");
  const pageStyles = `@page { size: A4; margin: 18mm; }
    body { font-family: ${BASE_FONT_STACK}; color: #111; line-height: 1.45; }
    h1, h2, h3 { margin: 0 0 0.75rem; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border: 1px solid #555; padding: 6px 8px; text-align: left; vertical-align: top; }
    .nowrap { white-space: nowrap; }
    .print-meta { margin-bottom: 1rem; }
    .print-meta p { margin: 0; }
    ${styles}`;
  const html = `<!DOCTYPE html><html lang="de"><head><meta charset="utf-8" />
    <title>${safeTitle}</title>
    <style>${pageStyles}</style>
  </head><body>${content}</body></html>`;
  const popup = openPopup(html, (blockedPopup) => {
    const overlay = ensureOverlay();
    overlay.innerHTML = `<style>${pageStyles}</style>${content}`;
    document.body.classList.add("print-overlay-active");
    const handleAfterPrint = () => {
      cleanupOverlay();
      window.removeEventListener("afterprint", handleAfterPrint);
    };
    window.addEventListener("afterprint", handleAfterPrint, { once: true });
    window.print();
    if (blockedPopup) {
      try {
        blockedPopup.close();
      } catch (err2) {
        console.warn("Unable to close blocked popup", err2);
      }
    }
  });
  if (!popup) {
    return;
  }
  const triggerPrint = () => {
    try {
      popup.focus();
    } catch (err2) {
      console.warn("Cannot focus print popup", err2);
    }
    try {
      popup.print();
    } catch (err2) {
      console.error("Popup print failed", err2);
    }
    const close = () => {
      try {
        popup.close();
      } catch (err2) {
        console.warn("Cannot close print popup", err2);
      }
    };
    if (popup.addEventListener) {
      popup.addEventListener("afterprint", close, { once: true });
    }
    setTimeout(close, 800);
  };
  if (popup.document.readyState === "complete") {
    setTimeout(triggerPrint, 50);
  } else {
    popup.addEventListener("load", () => {
      setTimeout(triggerPrint, 50);
    });
  }
}

// src/scripts/features/shared/printing.ts
var BASE_FONT_STACK2 = '"Helvetica Neue", Helvetica, Arial, sans-serif';
var PRINT_BASE_STYLES = `
  @page { size: A4; margin: 18mm; }
  body {
    font-family: ${BASE_FONT_STACK2};
    color: #111;
    line-height: 1.45;
    margin: 0;
    padding: 0;
  }
  h1, h2, h3 { margin: 0 0 0.75rem; }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 1rem;
  }
  th, td {
    border: 1px solid #555;
    padding: 6px 8px;
    text-align: left;
    vertical-align: top;
  }
  th {
    background: #f2f2f2;
    font-weight: 600;
  }
  .nowrap { white-space: nowrap; }
  .print-meta { margin-bottom: 1.5rem; }
  .print-meta p { margin: 0; }
  .print-meta h1 { margin-bottom: 0.5rem; }
  .calc-snapshot-print {
    page-break-inside: avoid;
    margin-bottom: 2rem;
    border-bottom: 2px solid #ddd;
    padding-bottom: 1rem;
  }
  .calc-snapshot-print:last-child {
    border-bottom: none;
  }
  .calc-snapshot-print__header h3 {
    margin: 0 0 0.5rem;
    font-size: 1.1rem;
  }
  .calc-snapshot-print__meta {
    margin-bottom: 1rem;
  }
  .calc-snapshot-print__meta p {
    margin: 0;
    font-size: 0.9rem;
  }
  .calc-snapshot-table,
  .history-detail-table {
    font-size: 0.9rem;
  }
`;
function createPrintFrame() {
  const iframe = document.createElement("iframe");
  iframe.style.position = "fixed";
  iframe.style.top = "-9999px";
  iframe.style.left = "-9999px";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);
  return iframe;
}
function cleanupPrintFrame(iframe) {
  if (iframe?.parentNode) {
    iframe.parentNode.removeChild(iframe);
  }
}
async function printEntriesChunked(entries, labels, options = {}) {
  const {
    chunkSize = 50,
    title = "Druck",
    headerHtml = "",
    additionalStyles = ""
  } = options;
  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error("No entries to print");
  }
  const iframe = createPrintFrame();
  const doc = iframe.contentDocument || iframe.contentWindow?.document;
  if (!doc) {
    cleanupPrintFrame(iframe);
    throw new Error("Print-Frame konnte nicht initialisiert werden");
  }
  const safeTitle = String(title).replace(/[<>]/g, "");
  const styles = PRINT_BASE_STYLES + (additionalStyles || "");
  doc.open();
  doc.write(`<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <title>${safeTitle}</title>
  <style>${styles}</style>
</head>
<body>`);
  if (headerHtml) {
    doc.write(headerHtml);
  }
  const container2 = doc.createElement("div");
  container2.className = "calc-snapshots-container";
  doc.body.appendChild(container2);
  const totalChunks = Math.ceil(entries.length / chunkSize);
  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex += 1) {
    const start = chunkIndex * chunkSize;
    const end = Math.min(start + chunkSize, entries.length);
    const chunk = entries.slice(start, end);
    const fragment = doc.createDocumentFragment();
    for (const entry of chunk) {
      const html = renderCalculationSnapshotForPrint(entry, labels);
      const wrapper = doc.createElement("div");
      wrapper.innerHTML = html;
      while (wrapper.firstChild) {
        fragment.appendChild(wrapper.firstChild);
      }
    }
    container2.appendChild(fragment);
    if (chunkIndex < totalChunks - 1) {
      await nextFrame();
    }
  }
  doc.write("</body></html>");
  doc.close();
  await new Promise((resolve) => {
    const frameWindow2 = iframe.contentWindow;
    if (!frameWindow2) {
      resolve();
      return;
    }
    if (frameWindow2.document.readyState === "complete") {
      resolve();
    } else {
      frameWindow2.addEventListener("load", () => resolve(), { once: true });
    }
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  try {
    iframe.contentWindow?.focus();
    iframe.contentWindow?.print();
  } catch (err2) {
    console.error("Print failed", err2);
    cleanupPrintFrame(iframe);
    throw err2;
  }
  const frameWindow = iframe.contentWindow;
  if (frameWindow && "addEventListener" in frameWindow) {
    frameWindow.addEventListener(
      "afterprint",
      () => cleanupPrintFrame(iframe),
      { once: true }
    );
  }
  setTimeout(() => cleanupPrintFrame(iframe), 5e3);
}

// src/scripts/features/history/index.ts
var USE_VIRTUAL_SCROLLING = true;
var INITIAL_LOAD_LIMIT = 200;
var initialized3 = false;
var virtualListInstance = null;
var selectedIndexes = /* @__PURE__ */ new Set();
async function persistHistoryChanges() {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === "memory") {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
  } catch (err2) {
    console.error("Fehler beim Persistieren der Historie", err2);
    window.alert(
      "Historie konnte nicht dauerhaft gespeichert werden. Bitte erneut versuchen."
    );
  }
}
function createSection2() {
  const wrapper = document.createElement("div");
  wrapper.className = "section-inner";
  wrapper.innerHTML = `
    <h2 class="text-center mb-4">Historie \u2013 Fr\xFChere Eintr\xE4ge</h2>
    <div class="card card-dark">
      <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
        <div class="small text-muted" data-role="selection-info">Keine Eintr\xE4ge ausgew\xE4hlt.</div>
        <button class="btn btn-outline-light btn-sm" data-action="print-selected" disabled>Ausgew\xE4hlte drucken</button>
      </div>
      <div class="card-body">
        <div data-role="history-list" class="history-list"></div>
      </div>
    </div>
    <div class="card card-dark mt-4 d-none" id="history-detail">
      <div class="card-header bg-info text-white">
        <h5 class="mb-0">Details</h5>
      </div>
      <div class="card-body" id="history-detail-body"></div>
    </div>
  `;
  return wrapper;
}
function updateCardSelection(listContainer, index, selected) {
  const card = listContainer?.querySelector(
    `.calc-snapshot-card[data-index="${index}"]`
  );
  if (!card) {
    return;
  }
  card.classList.toggle("calc-snapshot-card--selected", selected);
  const checkbox = card.querySelector(
    '[data-action="toggle-select"]'
  );
  if (checkbox) {
    checkbox.checked = selected;
  }
}
function renderCardsList(state2, listContainer, labels) {
  const entries = state2.history || [];
  const resolvedLabels = labels || getState().fieldLabels;
  for (const idx of Array.from(selectedIndexes)) {
    if (!entries[idx]) {
      selectedIndexes.delete(idx);
    }
  }
  if (USE_VIRTUAL_SCROLLING && entries.length > INITIAL_LOAD_LIMIT) {
    if (!virtualListInstance) {
      virtualListInstance = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 250,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = entries[index];
          const selected = selectedIndexes.has(index);
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: true,
            includeCheckbox: true,
            index,
            selected
          });
        }
      });
    } else {
      virtualListInstance.updateItemCount(entries.length);
    }
  } else {
    if (virtualListInstance) {
      virtualListInstance.destroy();
      virtualListInstance = null;
    }
    const limit = Math.min(entries.length, INITIAL_LOAD_LIMIT);
    const fragment = document.createDocumentFragment();
    for (let i = 0; i < limit; i += 1) {
      const entry = entries[i];
      const selected = selectedIndexes.has(i);
      const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
        showActions: true,
        includeCheckbox: true,
        index: i,
        selected
      });
      const wrapper = document.createElement("div");
      wrapper.innerHTML = cardHtml;
      if (wrapper.firstElementChild) {
        fragment.appendChild(wrapper.firstElementChild);
      }
    }
    listContainer.innerHTML = "";
    listContainer.appendChild(fragment);
    if (entries.length > limit) {
      const loadMoreBtn = document.createElement("button");
      loadMoreBtn.className = "btn btn-secondary w-100 mt-3";
      loadMoreBtn.textContent = `Mehr laden (${entries.length - limit} weitere)`;
      loadMoreBtn.dataset.action = "load-more";
      loadMoreBtn.dataset.currentLimit = String(limit);
      listContainer.appendChild(loadMoreBtn);
    }
  }
}
function renderHistoryTable(section, state2) {
  const listContainer = section.querySelector(
    '[data-role="history-list"]'
  );
  if (!listContainer) {
    return;
  }
  renderCardsList(state2, listContainer, state2.fieldLabels);
}
function renderDetail(entry, section, index, labels) {
  const detailCard = section.querySelector("#history-detail");
  const detailBody = section.querySelector("#history-detail-body");
  if (!detailCard || !detailBody) {
    return;
  }
  if (!entry) {
    detailCard.classList.add("d-none");
    detailBody.innerHTML = "";
    detailCard.dataset.index = "";
    return;
  }
  detailCard.dataset.index = index !== null ? String(index) : "";
  const resolvedLabels = labels || getState().fieldLabels;
  const tableLabels = resolvedLabels.history?.tableColumns ?? {};
  const detailLabels = resolvedLabels.history?.detail ?? {};
  const snapshotTable = buildMediumTableHTML(
    entry.items,
    resolvedLabels,
    "detail"
  );
  detailBody.innerHTML = `
    <p>
      <strong>${escapeHtml(tableLabels.date || "Datum")}:</strong> ${escapeHtml(entry.datum || entry.date || "")}<br />
      <strong>${escapeHtml(detailLabels.creator || "Erstellt von")}:</strong> ${escapeHtml(entry.ersteller || "")}<br />
      <strong>${escapeHtml(detailLabels.location || "Standort")}:</strong> ${escapeHtml(entry.standort || "")}<br />
      <strong>${escapeHtml(detailLabels.crop || "Kultur")}:</strong> ${escapeHtml(entry.kultur || "")}<br />
      <strong>${escapeHtml(detailLabels.quantity || "Kisten")}:</strong> ${escapeHtml(
    entry.kisten !== void 0 && entry.kisten !== null ? String(entry.kisten) : ""
  )}
    </p>
    <div class="table-responsive">
      ${snapshotTable}
    </div>
    <button class="btn btn-outline-secondary no-print" data-action="detail-print">Drucken / PDF</button>
  `;
  detailCard.classList.remove("d-none");
}
var HISTORY_SUMMARY_STYLES = `
  .history-summary {
    margin-top: 1.5rem;
  }
  .history-summary table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-summary th,
  .history-summary td {
    border: 1px solid #555;
    padding: 6px 8px;
    vertical-align: top;
  }
  .history-summary th {
    background: #f2f2f2;
  }
  .history-summary td div + div {
    margin-top: 0.25rem;
  }
  .history-detail h2 {
    margin-top: 1.5rem;
  }
  .history-detail table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-detail th,
  .history-detail td {
    border: 1px solid #555;
    padding: 6px 8px;
    text-align: left;
  }
  .nowrap {
    white-space: nowrap;
  }
`;
function buildCompanyHeader(company) {
  const hasContent = Boolean(
    company?.name || company?.headline || company?.address || company?.contactEmail
  );
  if (!hasContent) {
    return "";
  }
  const address = company?.address ? escapeHtml(company.address).replace(/\n/g, "<br />") : "";
  const email = company?.contactEmail ? `<p>${escapeHtml(company.contactEmail)}</p>` : "";
  return `
    <div class="print-meta">
      ${company?.name ? `<h1>${escapeHtml(company.name)}</h1>` : ""}
      ${company?.headline ? `<p>${escapeHtml(company.headline)}</p>` : ""}
      ${address ? `<p>${address}</p>` : ""}
      ${email}
    </div>
  `;
}
async function printSummary(entries, labels) {
  if (!entries.length) {
    window.alert("Keine Eintr\xE4ge zum Drucken ausgew\xE4hlt.");
    return;
  }
  const company = getState().company || {};
  const headerHtml = buildCompanyHeader(company);
  try {
    await printEntriesChunked(entries, labels, {
      title: "Historie \u2013 \xDCbersicht",
      headerHtml,
      chunkSize: 50
    });
  } catch (err2) {
    console.error("Printing failed", err2);
    window.alert("Fehler beim Drucken. Bitte erneut versuchen.");
  }
}
function printDetail(entry, labels) {
  if (!entry) {
    window.alert("Kein Eintrag zum Drucken vorhanden.");
    return;
  }
  const company = getState().company || {};
  const resolvedLabels = labels || getState().fieldLabels;
  const detailLabels = resolvedLabels.history?.detail ?? {};
  const snapshotTable = buildMediumTableHTML(
    entry.items || [],
    resolvedLabels,
    "detail",
    {
      classes: "history-detail-table"
    }
  );
  const content = `${buildCompanyHeader(company)}
    <section class="history-detail">
      <h2>${escapeHtml(detailLabels.title || "Historieneintrag")} \u2013 ${escapeHtml(
    entry.datum || entry.date || ""
  )}</h2>
      <p>
        <strong>${escapeHtml(detailLabels.creator || "Erstellt von")}:</strong> ${escapeHtml(
    entry.ersteller || ""
  )}<br />
        <strong>${escapeHtml(detailLabels.location || "Standort")}:</strong> ${escapeHtml(
    entry.standort || ""
  )}<br />
        <strong>${escapeHtml(detailLabels.crop || "Kultur")}:</strong> ${escapeHtml(
    entry.kultur || ""
  )}<br />
        <strong>${escapeHtml(detailLabels.quantity || "Kisten")}:</strong> ${escapeHtml(
    entry.kisten !== void 0 && entry.kisten !== null ? String(entry.kisten) : ""
  )}
      </p>
      ${snapshotTable}
    </section>
  `;
  printHtml({
    title: `Historie \u2013 ${entry.datum || entry.date || ""}`,
    styles: HISTORY_SUMMARY_STYLES,
    content
  });
}
function updateSelectionUI(section) {
  const info = section.querySelector(
    '[data-role="selection-info"]'
  );
  const printButton = section.querySelector(
    '[data-action="print-selected"]'
  );
  if (info) {
    info.textContent = selectedIndexes.size ? `${selectedIndexes.size} Eintrag(e) ausgew\xE4hlt.` : "Keine Eintr\xE4ge ausgew\xE4hlt.";
  }
  if (printButton) {
    printButton.disabled = !selectedIndexes.size;
  }
}
function toggleSectionAvailability(section, state2) {
  const hasDatabase = Boolean(state2.app?.hasDatabase);
  section.classList.toggle("d-none", !hasDatabase);
}
function initHistory(container2, services2) {
  if (!container2 || initialized3) {
    return;
  }
  const host = container2;
  host.innerHTML = "";
  const section = createSection2();
  host.appendChild(section);
  const handleStateChange = (nextState) => {
    toggleSectionAvailability(section, nextState);
    renderHistoryTable(section, nextState);
    const detailCard = section.querySelector("#history-detail");
    if (detailCard && !detailCard.classList.contains("d-none")) {
      const detailIndex = Number(detailCard.dataset.index);
      if (!Number.isNaN(detailIndex) && nextState.history[detailIndex]) {
        renderDetail(
          nextState.history[detailIndex],
          section,
          detailIndex,
          nextState.fieldLabels
        );
      } else {
        renderDetail(null, section, null, nextState.fieldLabels);
      }
    }
    updateSelectionUI(section);
  };
  services2.state.subscribe(handleStateChange);
  handleStateChange(services2.state.getState());
  section.addEventListener("click", (event) => {
    const target = event.target;
    if (!target) {
      return;
    }
    const action = target.dataset.action;
    if (!action) {
      return;
    }
    if (action === "detail-print") {
      const detailCard = target.closest("#history-detail");
      const indexAttr = detailCard?.dataset.index;
      const index2 = typeof indexAttr === "string" && indexAttr !== "" ? Number(indexAttr) : NaN;
      const state3 = services2.state.getState();
      const entry = Number.isInteger(index2) ? state3.history[index2] : null;
      printDetail(entry, state3.fieldLabels);
      return;
    }
    if (action === "print-selected") {
      const state3 = services2.state.getState();
      const entries = Array.from(selectedIndexes).sort((a, b) => a - b).map((idx) => state3.history[idx]).filter(Boolean);
      void printSummary(entries, state3.fieldLabels);
      return;
    }
    if (action === "load-more") {
      const btn = target;
      const currentLimit = parseInt(btn.dataset.currentLimit ?? "0", 10);
      const state3 = services2.state.getState();
      const newLimit = Math.min(
        state3.history.length,
        currentLimit + INITIAL_LOAD_LIMIT
      );
      const listContainer = section.querySelector(
        '[data-role="history-list"]'
      );
      if (!listContainer) {
        return;
      }
      const resolvedLabels = state3.fieldLabels;
      const fragment = document.createDocumentFragment();
      for (let i = currentLimit; i < newLimit; i += 1) {
        const entry = state3.history[i];
        const selected = selectedIndexes.has(i);
        const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
          showActions: true,
          includeCheckbox: true,
          index: i,
          selected
        });
        const wrapper = document.createElement("div");
        wrapper.innerHTML = cardHtml;
        if (wrapper.firstElementChild) {
          fragment.appendChild(wrapper.firstElementChild);
        }
      }
      btn.remove();
      listContainer.appendChild(fragment);
      if (newLimit < state3.history.length) {
        const newBtn = document.createElement("button");
        newBtn.className = "btn btn-secondary w-100 mt-3";
        newBtn.textContent = `Mehr laden (${state3.history.length - newLimit} weitere)`;
        newBtn.dataset.action = "load-more";
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      return;
    }
    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const state2 = services2.state.getState();
    if (action === "view") {
      const entry = state2.history[index];
      renderDetail(entry ?? null, section, index, state2.fieldLabels);
    } else if (action === "delete") {
      if (!window.confirm("Wirklich l\xF6schen?")) {
        return;
      }
      services2.state.updateSlice("history", (history) => {
        const copy = [...history];
        copy.splice(index, 1);
        return copy;
      });
      selectedIndexes.clear();
      updateSelectionUI(section);
      renderDetail(null, section, null, state2.fieldLabels);
      void persistHistoryChanges().catch((err2) => {
        console.error("Persist delete history error", err2);
      });
    }
  });
  section.addEventListener("change", (event) => {
    const target = event.target;
    if (!target || target.dataset.action !== "toggle-select") {
      return;
    }
    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const listContainer = section.querySelector('[data-role="history-list"]');
    if (target.checked) {
      selectedIndexes.add(index);
      updateCardSelection(listContainer, index, true);
    } else {
      selectedIndexes.delete(index);
      updateCardSelection(listContainer, index, false);
    }
    updateSelectionUI(section);
  });
  initialized3 = true;
}

// src/scripts/features/settings/index.ts
var initialized4 = false;
var mediumsTableBody = null;
var methodInput = null;
var methodDatalist = null;
var addForm = null;
function createSection3() {
  const section = document.createElement("div");
  section.className = "section-inner";
  section.innerHTML = `
    <h2 class="text-center mb-4">Mittel-Verwaltung</h2>
    <div class="card card-dark mb-4">
      <div class="card-body">
        <p class="mb-2"><strong>Was kann ich hier tun?</strong></p>
        <p class="text-muted mb-0">
          Erfasse, bearbeite oder l\xF6sche deine Mittel. Trage Name, Einheit, Methode und den Faktor ein und speichere
          die \xC4nderungen anschlie\xDFend in der Datenbank. Tippe bei der Methode einfach einen bestehenden Namen oder
          vergib einen neuen \u2013 neu erfasste Methoden stehen beim n\xE4chsten Mal automatisch zur Auswahl.
        </p>
      </div>
    </div>
    <div class="card card-dark mb-4">
      <div class="card-body">
        <div class="table-responsive">
          <table class="table table-dark table-bordered" id="settings-mediums-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Einheit</th>
                <th>Methode</th>
                <th>Wert</th>
                <th>Aktion</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="card card-dark">
      <div class="card-body">
        <h3 class="h5 text-success mb-3">Neues Mittel hinzuf\xFCgen</h3>
        <p class="text-muted">Trage alle Felder aus. Der Wert beschreibt den Faktor, der bei der Berechnung angewendet wird.</p>
        <form id="settings-medium-form" class="row g-3">
          <div class="col-md-3">
            <input class="form-control" name="medium-name" placeholder="Name (z. B. Elot-Vis)" required />
          </div>
          <div class="col-md-2">
            <input class="form-control" name="medium-unit" placeholder="Einheit (z. B. ml, %)" required />
          </div>
          <div class="col-md-3">
            <input class="form-control" name="medium-method" placeholder="Methode (z. B. perKiste)" list="settings-method-options" required />
            <datalist id="settings-method-options"></datalist>
          </div>
          <div class="col-md-2">
            <input type="number" step="any" class="form-control" name="medium-value" placeholder="Wert" required />
          </div>
          <div class="col-md-2 d-grid">
            <button class="btn btn-success" type="submit">Hinzuf\xFCgen</button>
          </div>
        </form>
        <div class="mt-3 small text-muted">
          Nach dem Hinzuf\xFCgen kannst du Mittel jederzeit l\xF6schen. \xC4nderungen werden erst mit dem Button unten dauerhaft gespeichert.
        </div>
      </div>
    </div>
    <div class="mt-4 no-print">
      <button class="btn btn-success" data-action="persist">\xC4nderungen speichern</button>
    </div>
  `;
  return section;
}
function renderMediumRows(state2) {
  if (!mediumsTableBody) {
    return;
  }
  const methodsById = new Map(
    state2.measurementMethods.map((method) => [method.id, method])
  );
  if (!state2.mediums.length) {
    mediumsTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="text-center text-muted">Noch keine Mittel gespeichert.</td>
      </tr>
    `;
    return;
  }
  mediumsTableBody.innerHTML = "";
  state2.mediums.forEach((medium, index) => {
    const row = document.createElement("tr");
    const method = methodsById.get(medium.methodId);
    row.innerHTML = `
      <td>${escapeHtml(medium.name)}</td>
      <td>${escapeHtml(medium.unit)}</td>
      <td>${escapeHtml(method ? method.label : medium.methodId)}</td>
      <td>${escapeHtml(medium.value != null ? String(medium.value) : "")}</td>
      <td>
        <button class="btn btn-sm btn-danger" data-action="delete" data-index="${index}">L\xF6schen</button>
      </td>
    `;
    mediumsTableBody?.appendChild(row);
  });
}
function renderMethodSuggestions(state2) {
  if (!methodDatalist) {
    return;
  }
  const seen = /* @__PURE__ */ new Set();
  methodDatalist.innerHTML = "";
  state2.measurementMethods.forEach((method) => {
    const labelKey = (method.label ?? "").toLowerCase();
    const idKey = (method.id ?? "").toLowerCase();
    if (labelKey && !seen.has(labelKey)) {
      seen.add(labelKey);
      const labelOption = document.createElement("option");
      labelOption.value = method.label;
      methodDatalist.appendChild(labelOption);
    }
    if (idKey && !seen.has(idKey)) {
      seen.add(idKey);
      const idOption = document.createElement("option");
      idOption.value = method.id;
      methodDatalist.appendChild(idOption);
    }
  });
}
function createMethodId(label) {
  const slug = label.toLowerCase().trim().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  if (slug) {
    return slug;
  }
  return `method-${Date.now()}-${Math.random().toString(16).slice(2, 6)}`;
}
function ensureMethodExists(state2, services2) {
  if (!methodInput) {
    return null;
  }
  const rawValue = methodInput.value.trim();
  if (!rawValue) {
    window.alert("Bitte eine Methode angeben.");
    methodInput.focus();
    return null;
  }
  const existing = state2.measurementMethods.find(
    (method) => method.label?.toLowerCase() === rawValue.toLowerCase() || method.id?.toLowerCase() === rawValue.toLowerCase()
  );
  if (existing) {
    return existing.id;
  }
  const id = createMethodId(rawValue);
  const defaultUnit = state2.fieldLabels?.calculation?.fields?.quantity?.unit || "Kiste";
  const newMethod = {
    id,
    label: rawValue,
    type: "factor",
    unit: defaultUnit,
    requires: ["kisten"],
    config: { sourceField: "kisten" }
  };
  services2.state.updateSlice("measurementMethods", (methods) => [
    ...methods,
    newMethod
  ]);
  return id;
}
async function persistChanges() {
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    window.alert("\xC4nderungen wurden gespeichert.");
  } catch (err2) {
    console.error("Fehler beim Speichern", err2);
    const message = err2 instanceof Error ? err2.message : "Speichern fehlgeschlagen";
    window.alert(message);
  }
}
function toggleSectionVisibility(section, state2) {
  const ready = Boolean(state2.app?.hasDatabase);
  const active = state2.app?.activeSection === "settings";
  section.classList.toggle("d-none", !(ready && active));
}
function initSettings(container2, services2) {
  if (!container2 || initialized4) {
    return;
  }
  const host = container2;
  host.innerHTML = "";
  const section = createSection3();
  host.appendChild(section);
  mediumsTableBody = section.querySelector(
    "#settings-mediums-table tbody"
  );
  methodInput = section.querySelector(
    'input[name="medium-method"]'
  );
  methodDatalist = section.querySelector(
    "#settings-method-options"
  );
  addForm = section.querySelector("#settings-medium-form");
  addForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const state2 = services2.state.getState();
    const formData = new FormData(addForm);
    const name = (formData.get("medium-name") || "").toString().trim();
    const unit = (formData.get("medium-unit") || "").toString().trim();
    const valueRaw = formData.get("medium-value");
    const value = Number(valueRaw);
    if (!name || !unit || Number.isNaN(value)) {
      window.alert("Bitte alle Felder korrekt ausf\xFCllen.");
      return;
    }
    const methodId = ensureMethodExists(state2, services2);
    if (!methodId) {
      return;
    }
    const id = typeof crypto !== "undefined" && typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `medium-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
    const medium = {
      id,
      name,
      unit,
      methodId,
      value
    };
    services2.state.updateSlice("mediums", (mediums) => [
      ...mediums,
      medium
    ]);
    addForm?.reset();
    renderMethodSuggestions(services2.state.getState());
  });
  mediumsTableBody?.addEventListener("click", (event) => {
    const target = event.target?.closest('[data-action="delete"]');
    if (!target) {
      return;
    }
    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    services2.state.updateSlice("mediums", (mediums) => {
      const copy = mediums.slice();
      copy.splice(index, 1);
      return copy;
    });
  });
  section.querySelector('[data-action="persist"]')?.addEventListener("click", () => {
    void persistChanges();
  });
  const handleStateChange = (nextState) => {
    toggleSectionVisibility(section, nextState);
    if (nextState.app.activeSection === "settings") {
      renderMediumRows(nextState);
      renderMethodSuggestions(nextState);
    }
  };
  services2.state.subscribe(handleStateChange);
  handleStateChange(services2.state.getState());
  initialized4 = true;
}

// src/scripts/features/reporting/index.ts
var USE_VIRTUAL_SCROLLING2 = true;
var INITIAL_LOAD_LIMIT2 = 200;
var initialized5 = false;
var currentEntries = [];
var activeFilter = null;
var virtualListInstance2 = null;
function createSection4() {
  const section = document.createElement("div");
  section.className = "section-inner";
  section.innerHTML = `
    <h2 class="text-center mb-4">Auswertung nach Datum</h2>
    <div class="card card-dark no-print mb-4">
      <div class="card-body">
        <form id="report-filter" class="row g-3">
          <div class="col-md-4">
            <label class="form-label" for="report-start">Startdatum</label>
            <input type="date" class="form-control" id="report-start" name="report-start" required />
          </div>
          <div class="col-md-4">
            <label class="form-label" for="report-end">Enddatum</label>
            <input type="date" class="form-control" id="report-end" name="report-end" required />
          </div>
          <div class="col-md-4 d-flex align-items-end">
            <button class="btn btn-success w-100" type="submit">Anzeigen</button>
          </div>
        </form>
      </div>
    </div>
    <div class="card card-dark">
      <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
        <div class="small text-muted" data-role="report-info">Alle Eintr\xE4ge</div>
        <button class="btn btn-outline-light btn-sm" data-action="print-report" disabled>Drucken</button>
      </div>
      <div class="card-body">
        <div data-role="report-list" class="report-list"></div>
      </div>
    </div>
  `;
  return section;
}
function parseDate(value) {
  if (typeof value !== "string" || !value) {
    return null;
  }
  const parts = value.split("-");
  if (parts.length !== 3) {
    return null;
  }
  const [year, month, day] = parts.map(Number);
  if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) {
    return null;
  }
  return new Date(year, month - 1, day);
}
function germanDateToIso(value) {
  if (!value) {
    return null;
  }
  const parts = value.split(".");
  if (parts.length !== 3) {
    return null;
  }
  const [day, month, year] = parts.map(Number);
  if (Number.isNaN(day) || Number.isNaN(month) || Number.isNaN(year)) {
    return null;
  }
  return new Date(year, month - 1, day);
}
function resolveReportingLabels(labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  return resolvedLabels?.reporting || {};
}
function describeFilter(entryCount, labels) {
  const reportingLabels = resolveReportingLabels(labels);
  const infoEmpty = reportingLabels.infoEmpty || "Keine Eintr\xE4ge";
  const infoAll = reportingLabels.infoAll || "Alle Eintr\xE4ge";
  const infoPrefix = reportingLabels.infoPrefix || "Auswahl";
  if (!activeFilter) {
    if (!entryCount) {
      return infoEmpty;
    }
    return `${infoAll} (${entryCount})`;
  }
  const prefix = `${infoPrefix} ${activeFilter.startLabel} \u2013 ${activeFilter.endLabel}`;
  if (!entryCount) {
    return `${prefix} (${infoEmpty})`;
  }
  return `${prefix} (${entryCount})`;
}
function renderCardsList2(listContainer, entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  if (USE_VIRTUAL_SCROLLING2 && entries.length > INITIAL_LOAD_LIMIT2) {
    listContainer.innerHTML = "";
    if (!virtualListInstance2) {
      virtualListInstance2 = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 200,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = currentEntries[index];
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: false,
            includeCheckbox: false
          });
        }
      });
    } else {
      virtualListInstance2.updateItemCount(entries.length);
    }
    return;
  }
  if (virtualListInstance2) {
    virtualListInstance2.destroy();
    virtualListInstance2 = null;
  }
  const limit = Math.min(entries.length, INITIAL_LOAD_LIMIT2);
  const fragment = document.createDocumentFragment();
  for (let i = 0; i < limit; i += 1) {
    const entry = entries[i];
    const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
      showActions: false,
      includeCheckbox: false
    });
    const wrapper = document.createElement("div");
    wrapper.innerHTML = cardHtml;
    if (wrapper.firstElementChild) {
      fragment.appendChild(wrapper.firstElementChild);
    }
  }
  listContainer.innerHTML = "";
  listContainer.appendChild(fragment);
  if (entries.length > limit) {
    const loadMoreBtn = document.createElement("button");
    loadMoreBtn.className = "btn btn-secondary w-100 mt-3";
    loadMoreBtn.textContent = `Mehr laden (${entries.length - limit} weitere)`;
    loadMoreBtn.dataset.action = "load-more";
    loadMoreBtn.dataset.currentLimit = String(limit);
    listContainer.appendChild(loadMoreBtn);
  }
}
function renderTable(section, entries, labels) {
  currentEntries = entries.slice();
  const listContainer = section.querySelector(
    '[data-role="report-list"]'
  );
  if (listContainer) {
    renderCardsList2(listContainer, entries, labels);
  }
  const info = section.querySelector('[data-role="report-info"]');
  if (info) {
    info.textContent = describeFilter(entries.length, labels);
  }
  const printButton = section.querySelector(
    '[data-action="print-report"]'
  );
  if (printButton) {
    printButton.disabled = entries.length === 0;
  }
}
function applyFilter(section, state2, filter) {
  const source = Array.isArray(state2.history) ? state2.history : [];
  if (!filter) {
    renderTable(section, source, state2.fieldLabels);
    return;
  }
  const filtered = source.filter((entry) => {
    const isoDate = germanDateToIso(
      entry.datum || entry.date
    );
    if (!isoDate) {
      return false;
    }
    return isoDate >= filter.start && isoDate <= filter.end;
  });
  renderTable(section, filtered, state2.fieldLabels);
}
function toggleSection(section, state2) {
  const ready = Boolean(state2.app?.hasDatabase);
  const active = state2.app?.activeSection === "report";
  section.classList.toggle("d-none", !(ready && active));
  if (ready) {
    applyFilter(section, state2, activeFilter);
  }
}
function buildCompanyHeader2(company) {
  const hasContent = Boolean(
    company?.name || company?.headline || company?.address || company?.contactEmail
  );
  if (!hasContent) {
    return "";
  }
  const address = company?.address ? escapeHtml(company.address).replace(/\n/g, "<br />") : "";
  const email = company?.contactEmail ? `<p>${escapeHtml(company.contactEmail)}</p>` : "";
  return `
    <div class="print-meta">
      ${company?.name ? `<h1>${escapeHtml(company.name)}</h1>` : ""}
      ${company?.headline ? `<p>${escapeHtml(company.headline)}</p>` : ""}
      ${address ? `<p>${address}</p>` : ""}
      ${email}
    </div>
  `;
}
function buildFilterInfo(filter, labels) {
  const reportingLabels = resolveReportingLabels(labels);
  const prefix = escapeHtml(reportingLabels.infoPrefix || "Auswahl");
  if (!filter) {
    const infoAll = escapeHtml(reportingLabels.infoAll || "Alle Eintr\xE4ge");
    return `<p>${prefix}: ${infoAll}</p>`;
  }
  return `<p>${prefix}: ${escapeHtml(filter.startLabel)} \u2013 ${escapeHtml(filter.endLabel)}</p>`;
}
async function printReport(entries, filter, labels) {
  if (!entries.length) {
    window.alert("Keine Daten f\xFCr den Druck vorhanden.");
    return;
  }
  const resolvedLabels = labels || getState().fieldLabels;
  const reportingLabels = resolveReportingLabels(resolvedLabels);
  const company = getState().company || {};
  const headerHtml = buildCompanyHeader2(company) + buildFilterInfo(filter, resolvedLabels);
  try {
    await printEntriesChunked(entries, resolvedLabels, {
      title: reportingLabels.printTitle || "Bericht",
      headerHtml,
      chunkSize: 50
    });
  } catch (err2) {
    console.error("Printing failed", err2);
    window.alert("Fehler beim Drucken. Bitte erneut versuchen.");
  }
}
function initReporting(container2, services2) {
  if (!container2 || initialized5) {
    return;
  }
  const host = container2;
  host.innerHTML = "";
  const section = createSection4();
  host.appendChild(section);
  const filterForm = section.querySelector("#report-filter");
  filterForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(filterForm);
    const start = parseDate(formData.get("report-start"));
    const end = parseDate(formData.get("report-end"));
    if (!start || !end) {
      window.alert("Bitte g\xFCltige Daten ausw\xE4hlen!");
      return;
    }
    if (start > end) {
      window.alert("Das Startdatum muss vor dem Enddatum liegen.");
      return;
    }
    activeFilter = {
      start,
      end,
      startLabel: new Intl.DateTimeFormat("de-DE").format(start),
      endLabel: new Intl.DateTimeFormat("de-DE").format(end)
    };
    applyFilter(section, services2.state.getState(), activeFilter);
  });
  services2.state.subscribe((nextState) => {
    toggleSection(section, nextState);
  });
  toggleSection(section, services2.state.getState());
  section.addEventListener("click", (event) => {
    const target = event.target;
    if (!target) {
      return;
    }
    if (target.dataset.action === "load-more") {
      const btn = target;
      const currentLimit = parseInt(btn.dataset.currentLimit || "0", 10);
      const listContainer = section.querySelector(
        '[data-role="report-list"]'
      );
      if (!listContainer) {
        return;
      }
      const newLimit = Math.min(
        currentEntries.length,
        currentLimit + INITIAL_LOAD_LIMIT2
      );
      const labels = services2.state.getState().fieldLabels;
      const fragment = document.createDocumentFragment();
      for (let i = currentLimit; i < newLimit; i += 1) {
        const entry = currentEntries[i];
        const cardHtml = renderCalculationSnapshot(entry, labels, {
          showActions: false,
          includeCheckbox: false
        });
        const wrapper = document.createElement("div");
        wrapper.innerHTML = cardHtml;
        if (wrapper.firstElementChild) {
          fragment.appendChild(wrapper.firstElementChild);
        }
      }
      btn.remove();
      listContainer.appendChild(fragment);
      if (newLimit < currentEntries.length) {
        const newBtn = document.createElement("button");
        newBtn.className = "btn btn-secondary w-100 mt-3";
        newBtn.textContent = `Mehr laden (${currentEntries.length - newLimit} weitere)`;
        newBtn.dataset.action = "load-more";
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      return;
    }
    const printTrigger = target.closest(
      '[data-action="print-report"]'
    );
    if (printTrigger) {
      void printReport(
        currentEntries,
        activeFilter,
        services2.state.getState().fieldLabels
      );
    }
  });
  initialized5 = true;
}

// src/scripts/core/bvlClient.ts
var BVL_BASE_URL = "https://psm-api.bvl.bund.de/ords/psm/api-v1";
var DEFAULT_TIMEOUT = 3e4;
var MAX_RETRIES = 2;
var NetworkError = class extends Error {
  constructor(message, endpoint) {
    super(message);
    this.name = "NetworkError";
    this.endpoint = endpoint;
  }
};
var HttpError = class extends Error {
  constructor(message, status, endpoint, responseBody) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.endpoint = endpoint;
    this.responseBody = responseBody;
  }
};
var ParseError = class extends Error {
  constructor(message, endpoint) {
    super(message);
    this.name = "ParseError";
    this.endpoint = endpoint;
  }
};
async function fetchCollection(endpoint, options = {}) {
  const {
    timeout = DEFAULT_TIMEOUT,
    maxRetries = MAX_RETRIES,
    onProgress = null,
    params = {},
    pageSize = 1e3
  } = options;
  let allItems = [];
  let offset = 0;
  let hasMore = true;
  let attempt = 0;
  while (hasMore) {
    attempt = 0;
    let success = false;
    let lastError = null;
    while (!success && attempt <= maxRetries) {
      try {
        const searchParams = new URLSearchParams();
        for (const [key, value] of Object.entries(params)) {
          if (value === void 0 || value === null) {
            continue;
          }
          searchParams.append(key, value);
        }
        searchParams.set("limit", pageSize);
        searchParams.set("offset", offset);
        const url = `${BVL_BASE_URL}/${endpoint}?${searchParams.toString()}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        const startTime = Date.now();
        const response = await fetch(url, {
          signal: controller.signal,
          headers: {
            Accept: "application/json"
          }
        });
        clearTimeout(timeoutId);
        const duration = Date.now() - startTime;
        if (!response.ok) {
          const responseText = await response.text().catch(() => "");
          const truncatedBody = responseText.length > 200 ? responseText.substring(0, 200) + "..." : responseText;
          if (response.status >= 500 && attempt < maxRetries) {
            attempt++;
            lastError = new HttpError(
              `HTTP ${response.status} on attempt ${attempt}`,
              response.status,
              endpoint,
              truncatedBody
            );
            await new Promise((resolve) => setTimeout(resolve, 1e3 * attempt));
            continue;
          }
          throw new HttpError(
            `HTTP ${response.status}: ${response.statusText}`,
            response.status,
            endpoint,
            truncatedBody
          );
        }
        let data;
        try {
          data = await response.json();
        } catch (e) {
          throw new ParseError(
            `Failed to parse JSON response: ${e.message}`,
            endpoint
          );
        }
        const items = data.items || [];
        allItems = allItems.concat(items);
        if (onProgress) {
          onProgress({
            endpoint,
            offset,
            count: items.length,
            total: allItems.length,
            duration
          });
        }
        if (items.length < pageSize) {
          hasMore = false;
        } else {
          offset += pageSize;
        }
        success = true;
      } catch (error) {
        if (error.name === "AbortError") {
          throw new NetworkError(
            `Request timeout after ${timeout}ms`,
            endpoint
          );
        }
        if (error instanceof HttpError || error instanceof ParseError) {
          throw error;
        }
        if (!navigator.onLine) {
          throw new NetworkError("No internet connection", endpoint);
        }
        if (attempt < maxRetries) {
          attempt++;
          lastError = error;
          await new Promise((resolve) => setTimeout(resolve, 1e3 * attempt));
        } else {
          throw new NetworkError(
            `Network error after ${attempt} attempts: ${error.message}`,
            endpoint
          );
        }
      }
    }
    if (!success && lastError) {
      throw lastError;
    }
  }
  return allItems;
}
async function hashData(data) {
  const text = typeof data === "string" ? data : JSON.stringify(data);
  const msgBuffer = new TextEncoder().encode(text);
  const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
  return hashHex;
}
async function computeDatasetHashes(datasets) {
  const hashes = {};
  const combined = [];
  for (const [endpoint, data] of Object.entries(datasets)) {
    const hash = await hashData(data);
    hashes[endpoint] = hash;
    combined.push(hash);
  }
  hashes.combined = await hashData(combined.join(""));
  return hashes;
}

// src/scripts/core/bvlDataset.ts
var DEFAULT_MANIFEST_URL = "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/manifest.json";
var MANIFEST_STORAGE_KEY = "bvlManifestUrl";
var ManifestError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "ManifestError";
  }
};
var DownloadError = class extends Error {
  constructor(message, url) {
    super(message);
    this.name = "DownloadError";
    this.url = url;
  }
};
var fflateLoader = null;
async function loadFflate() {
  if (typeof window !== "undefined" && window.fflate) {
    return window.fflate;
  }
  if (!fflateLoader) {
    fflateLoader = (async () => {
      try {
        return await Promise.resolve().then(() => (init_browser(), browser_exports));
      } catch (error) {
        return await import("https://cdn.jsdelivr.net/npm/fflate@0.8.1/esm/browser.js");
      }
    })();
  }
  return fflateLoader;
}
function getManifestUrl() {
  try {
    const stored = localStorage.getItem(MANIFEST_STORAGE_KEY);
    if (stored && stored.trim()) {
      return stored.trim();
    }
  } catch (e) {
    console.warn("Could not access localStorage for manifest URL", e);
  }
  return DEFAULT_MANIFEST_URL;
}
async function fetchManifest(options = {}) {
  const { timeout = 3e4 } = options;
  const manifestUrl = getManifestUrl();
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(manifestUrl, {
      signal: controller.signal,
      headers: {
        Accept: "application/json"
      }
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      throw new ManifestError(
        `Failed to fetch manifest: HTTP ${response.status}`
      );
    }
    const manifest = await response.json();
    if (!manifest.version) {
      throw new ManifestError("Manifest missing required field: version");
    }
    if (!manifest.files || !Array.isArray(manifest.files)) {
      throw new ManifestError("Manifest missing required field: files");
    }
    try {
      manifest.manifestUrl = manifestUrl;
      const baseCandidate = manifest.baseUrl || manifest.base_url || "./";
      manifest.baseUrlResolved = new URL(baseCandidate, manifestUrl).toString();
    } catch (resolveError) {
      console.warn(
        "Failed to resolve manifest base URL, using default",
        resolveError
      );
      manifest.baseUrlResolved = "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";
    }
    return manifest;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new ManifestError(`Manifest fetch timeout after ${timeout}ms`);
    }
    if (error instanceof ManifestError) {
      throw error;
    }
    throw new ManifestError(`Failed to fetch manifest: ${error.message}`);
  }
}
function supportsStreamDecompression(kind) {
  try {
    if (typeof DecompressionStream === "undefined") {
      return false;
    }
    const stream = new DecompressionStream(kind);
    return !!stream;
  } catch (e) {
    return false;
  }
}
function selectBestFile(manifest) {
  const files = manifest.files;
  if (supportsStreamDecompression("br")) {
    const brFile = files.find((f) => f.path.endsWith(".sqlite.br"));
    if (brFile) {
      return { file: brFile, format: "brotli" };
    }
  }
  const gzFile = files.find((f) => f.path.endsWith(".sqlite.gz"));
  if (gzFile) {
    return { file: gzFile, format: "gzip" };
  }
  const sqliteFile = files.find(
    (f) => f.path.endsWith(".sqlite") && !f.path.includes(".")
  );
  if (sqliteFile) {
    return { file: sqliteFile, format: "plain" };
  }
  const zipFile = files.find((f) => f.path.endsWith(".sqlite.zip"));
  if (zipFile) {
    return { file: zipFile, format: "zip" };
  }
  throw new ManifestError("No suitable database file found in manifest");
}
async function downloadFile(url, options = {}) {
  const { onProgress = null, timeout = 12e4 } = options;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, {
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      throw new DownloadError(`Download failed: HTTP ${response.status}`, url);
    }
    const contentLength = parseInt(
      response.headers.get("content-length") || "0",
      10
    );
    const reader = response.body.getReader();
    const chunks = [];
    let receivedLength = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      chunks.push(value);
      receivedLength += value.length;
      if (onProgress && contentLength > 0) {
        onProgress({
          loaded: receivedLength,
          total: contentLength,
          percent: Math.round(receivedLength / contentLength * 100)
        });
      }
    }
    const result = new Uint8Array(receivedLength);
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }
    return result;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new DownloadError(`Download timeout after ${timeout}ms`, url);
    }
    if (error instanceof DownloadError) {
      throw error;
    }
    throw new DownloadError(`Download failed: ${error.message}`, url);
  }
}
async function decompressBrotli(compressedData) {
  try {
    const ds = new DecompressionStream("br");
    const writer = ds.writable.getWriter();
    const reader = ds.readable.getReader();
    writer.write(compressedData);
    writer.close();
    const chunks = [];
    let totalLength = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      totalLength += value.length;
    }
    const result = new Uint8Array(totalLength);
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }
    return result;
  } catch (error) {
    throw new Error(`Brotli decompression failed: ${error.message}`);
  }
}
async function decompressGzip(compressedData) {
  try {
    if (supportsStreamDecompression("gzip")) {
      const ds = new DecompressionStream("gzip");
      const writer = ds.writable.getWriter();
      const reader = ds.readable.getReader();
      writer.write(compressedData);
      writer.close();
      const chunks = [];
      let totalLength = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        totalLength += value.length;
      }
      const result = new Uint8Array(totalLength);
      let position = 0;
      for (const chunk of chunks) {
        result.set(chunk, position);
        position += chunk.length;
      }
      return result;
    }
    const fflate = await loadFflate();
    const gunzipSync2 = fflate.gunzipSync || null;
    if (gunzipSync2) {
      return gunzipSync2(compressedData);
    }
    if (typeof fflate.gunzip === "function") {
      return await new Promise((resolve, reject) => {
        fflate.gunzip(compressedData, (err2, result) => {
          if (err2) {
            reject(err2);
          } else {
            resolve(result);
          }
        });
      });
    }
    throw new Error("No gzip decompressor available");
  } catch (error) {
    throw new Error(`Gzip decompression failed: ${error.message}`);
  }
}
async function decompressZip(compressedData) {
  throw new Error(
    "ZIP decompression not yet implemented. Please use .sqlite or .sqlite.br format."
  );
}
async function downloadDatabase(manifest, options = {}) {
  const { onProgress = null } = options;
  let baseUrl = manifest.baseUrlResolved || manifest.baseUrl || manifest.base_url || "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";
  try {
    baseUrl = new URL(baseUrl, manifest.manifestUrl || baseUrl).toString();
  } catch (resolveError) {
    console.warn(
      "Failed to normalize base URL, falling back to default",
      resolveError
    );
    baseUrl = "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";
  }
  const { file, format } = selectBestFile(manifest);
  const fileUrl = new URL(file.path, baseUrl).toString();
  if (onProgress) {
    onProgress({
      step: "download",
      message: `Lade Datenbank (${format})...`,
      percent: 0
    });
  }
  const compressed = await downloadFile(fileUrl, {
    onProgress: (progress) => {
      if (onProgress) {
        onProgress({
          step: "download",
          message: `Lade Datenbank: ${progress.percent}%`,
          percent: Math.round(progress.percent * 0.7),
          // Reserve 30% for decompression
          ...progress
        });
      }
    }
  });
  let sqliteData;
  if (format === "brotli") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70
      });
    }
    sqliteData = await decompressBrotli(compressed);
  } else if (format === "gzip") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70
      });
    }
    sqliteData = await decompressGzip(compressed);
  } else if (format === "zip") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70
      });
    }
    sqliteData = await decompressZip(compressed);
  } else {
    sqliteData = compressed;
  }
  if (onProgress) {
    onProgress({
      step: "complete",
      message: "Datenbank heruntergeladen",
      percent: 100
    });
  }
  return {
    data: sqliteData,
    file,
    format,
    manifest
  };
}
async function checkForUpdates(currentHash) {
  try {
    const manifest = await fetchManifest();
    const manifestHash = manifest.hash || manifest.version;
    return {
      available: currentHash !== manifestHash,
      manifest,
      newVersion: manifestHash
    };
  } catch (error) {
    console.warn("Failed to check for updates:", error);
    return {
      available: false,
      manifest: null,
      newVersion: null,
      error: error.message
    };
  }
}

// src/scripts/core/bvlSync.ts
var USE_MANIFEST_SYNC = true;
var SYNC_ENDPOINTS = [
  { key: "mittel", endpoint: "mittel", category: "core", label: "Mittel" },
  { key: "awg", endpoint: "awg", category: "core", label: "Anwendungen" },
  {
    key: "awg_kultur",
    endpoint: "awg_kultur",
    category: "core",
    label: "Anwendungs-Kulturen"
  },
  {
    key: "awg_schadorg",
    endpoint: "awg_schadorg",
    category: "core",
    label: "Anwendungs-Schadorganismen"
  },
  {
    key: "awg_aufwand",
    endpoint: "awg_aufwand",
    category: "core",
    label: "Anwendungs-Aufw\xE4nde"
  },
  {
    key: "awg_wartezeit",
    endpoint: "awg_wartezeit",
    category: "core",
    label: "Wartezeiten"
  },
  {
    key: "adresse",
    endpoint: "adresse",
    category: "payload",
    label: "Adressen",
    primaryRefField: "ADRESSE_NR"
  },
  {
    key: "antrag",
    endpoint: "antrag",
    category: "payload",
    label: "Antr\xE4ge",
    primaryRefField: "KENNR",
    secondaryRefField: "ANTRAGNR"
  },
  {
    key: "auflage_redu",
    endpoint: "auflage_redu",
    category: "payload",
    label: "Auflagen reduziert",
    primaryRefField: "AUFLAGENR"
  },
  {
    key: "auflagen",
    endpoint: "auflagen",
    category: "payload",
    label: "Auflagen",
    primaryRefField: "KENNR",
    secondaryRefField: "EBENE"
  },
  {
    key: "awg_bem",
    endpoint: "awg_bem",
    category: "payload",
    label: "Anwendungs-Bemerkungen",
    primaryRefField: "AWG_ID"
  },
  {
    key: "awg_partner",
    endpoint: "awg_partner",
    category: "payload",
    label: "Anwendungs-Partner",
    primaryRefField: "AWG_ID",
    secondaryRefField: "KENNR_PARTNER"
  },
  {
    key: "awg_partner_aufwand",
    endpoint: "awg_partner_aufwand",
    category: "payload",
    label: "Partner-Aufw\xE4nde",
    primaryRefField: "AWG_ID",
    secondaryRefField: "KENNR_PARTNER"
  },
  {
    key: "awg_verwendungszweck",
    endpoint: "awg_verwendungszweck",
    category: "payload",
    label: "Verwendungszwecke",
    primaryRefField: "AWG_ID"
  },
  {
    key: "awg_wartezeit_ausg_kultur",
    endpoint: "awg_wartezeit_ausg_kultur",
    category: "payload",
    label: "Wartezeit-Ausnahmen",
    primaryRefField: "AWG_ID"
  },
  {
    key: "awg_zeitpunkt",
    endpoint: "awg_zeitpunkt",
    category: "payload",
    label: "Anwendungszeitpunkte",
    primaryRefField: "AWG_ID"
  },
  {
    key: "awg_zulassung",
    endpoint: "awg_zulassung",
    category: "payload",
    label: "Zulassungsdetails",
    primaryRefField: "AWG_ID"
  },
  {
    key: "ghs_gefahrenhinweise",
    endpoint: "ghs_gefahrenhinweise",
    category: "payload",
    label: "GHS Gefahrenhinweise",
    primaryRefField: "KENNR"
  },
  {
    key: "ghs_gefahrensymbole",
    endpoint: "ghs_gefahrensymbole",
    category: "payload",
    label: "GHS Symbole",
    primaryRefField: "KENNR"
  },
  {
    key: "ghs_sicherheitshinweise",
    endpoint: "ghs_sicherheitshinweise",
    category: "payload",
    label: "GHS Sicherheitshinweise",
    primaryRefField: "KENNR"
  },
  {
    key: "ghs_signalwoerter",
    endpoint: "ghs_signalwoerter",
    category: "payload",
    label: "GHS Signalw\xF6rter",
    primaryRefField: "KENNR"
  },
  {
    key: "hinweis",
    endpoint: "hinweis",
    category: "payload",
    label: "Hinweise",
    primaryRefField: "KENNR"
  },
  {
    key: "kodeliste",
    endpoint: "kodeliste",
    category: "payload",
    label: "Kodlisten",
    primaryRefField: "KODELISTE_NR"
  },
  {
    key: "kodeliste_feldname",
    endpoint: "kodeliste_feldname",
    category: "payload",
    label: "Feldnamen",
    primaryRefField: "FELD"
  },
  {
    key: "kultur_gruppe",
    endpoint: "kultur_gruppe",
    category: "payload",
    label: "Kulturgruppen",
    primaryRefField: "GRUPPE"
  },
  {
    key: "mittel_abgelaufen",
    endpoint: "mittel_abgelaufen",
    category: "payload",
    label: "Abgelaufene Mittel",
    primaryRefField: "KENNR"
  },
  {
    key: "mittel_abpackung",
    endpoint: "mittel_abpackung",
    category: "payload",
    label: "Abpackungen",
    primaryRefField: "KENNR"
  },
  {
    key: "mittel_gefahren_symbol",
    endpoint: "mittel_gefahren_symbol",
    category: "payload",
    label: "Gefahrensymbole",
    primaryRefField: "KENNR"
  },
  {
    key: "mittel_vertrieb",
    endpoint: "mittel_vertrieb",
    category: "payload",
    label: "Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR"
  },
  {
    key: "mittel_wirkbereich",
    endpoint: "mittel_wirkbereich",
    category: "payload",
    label: "Wirkbereiche",
    primaryRefField: "KENNR"
  },
  {
    key: "parallelimport_abgelaufen",
    endpoint: "parallelimport_abgelaufen",
    category: "payload",
    label: "Parallelimport (alt)",
    primaryRefField: "KENNR"
  },
  {
    key: "parallelimport_gueltig",
    endpoint: "parallelimport_gueltig",
    category: "payload",
    label: "Parallelimport (g\xFCltig)",
    primaryRefField: "KENNR"
  },
  {
    key: "schadorg_gruppe",
    endpoint: "schadorg_gruppe",
    category: "payload",
    label: "Schadorganismus-Gruppen",
    primaryRefField: "GRUPPE"
  },
  {
    key: "staerkung",
    endpoint: "staerkung",
    category: "payload",
    label: "Pflanzenst\xE4rkung",
    primaryRefField: "KENNR"
  },
  {
    key: "staerkung_vertrieb",
    endpoint: "staerkung_vertrieb",
    category: "payload",
    label: "Pflanzenst\xE4rkung Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR"
  },
  {
    key: "stand",
    endpoint: "stand",
    category: "payload",
    label: "API Stand"
  },
  {
    key: "wirkstoff",
    endpoint: "wirkstoff",
    category: "payload",
    label: "Wirkstoffe",
    primaryRefField: "KENNR"
  },
  {
    key: "wirkstoff_gehalt",
    endpoint: "wirkstoff_gehalt",
    category: "payload",
    label: "Wirkstoffgehalt",
    primaryRefField: "KENNR"
  },
  {
    key: "zusatzstoff",
    endpoint: "zusatzstoff",
    category: "payload",
    label: "Zusatzstoffe",
    primaryRefField: "KENNR"
  },
  {
    key: "zusatzstoff_vertrieb",
    endpoint: "zusatzstoff_vertrieb",
    category: "payload",
    label: "Zusatzstoff Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR"
  }
];
var LOOKUP_CONFIG = [
  {
    key: "lookupCultures",
    endpoint: "kode",
    params: { kodeliste: 948, sprache: "DE" },
    progressKey: "lookup:kulturen",
    label: "Kulturen (Klartexte)"
  },
  {
    key: "lookupPests",
    endpoint: "kode",
    params: { kodeliste: 947, sprache: "DE" },
    progressKey: "lookup:schadorg",
    label: "Schadorganismen (Klartexte)"
  }
];
function extractFieldValue(item, fieldName) {
  if (!item || !fieldName) {
    return null;
  }
  const candidates = [
    fieldName,
    fieldName.toLowerCase(),
    fieldName.toUpperCase()
  ];
  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(item, key)) {
      const value = item[key];
      if (value === null || value === void 0 || value === "") {
        return null;
      }
      return String(value);
    }
  }
  return null;
}
async function syncBvlData(storage, options = {}) {
  const { onProgress = () => {
  }, onLog = () => {
  } } = options;
  const startTime = Date.now();
  const log = (level, message, data = null) => {
    onLog({
      level,
      message,
      data,
      timestamp: (/* @__PURE__ */ new Date()).toISOString()
    });
  };
  try {
    onProgress({
      step: "start",
      percent: 0,
      message: "Starte Synchronisation..."
    });
    log("info", "Starting BVL data sync");
    if (!navigator.onLine) {
      throw new Error("Keine Internetverbindung verf\xFCgbar");
    }
    if (USE_MANIFEST_SYNC) {
      return await syncFromManifest(storage, {
        onProgress,
        onLog,
        log,
        startTime
      });
    }
    return await syncFromApi(storage, { onProgress, onLog, log, startTime });
  } catch (error) {
    const errorMessage = error.message || "Unbekannter Fehler";
    log("error", "Sync failed", { error: errorMessage, stack: error.stack });
    const meta = {
      lastError: errorMessage,
      lastErrorTime: (/* @__PURE__ */ new Date()).toISOString()
    };
    try {
      await storage.setBvlMeta("lastError", errorMessage);
      await storage.appendBvlSyncLog({
        synced_at: (/* @__PURE__ */ new Date()).toISOString(),
        ok: 0,
        message: errorMessage,
        payload_hash: null
      });
    } catch (e) {
      log("error", "Failed to write error state", { error: e.message });
    }
    onProgress({
      step: "error",
      percent: 0,
      message: `Fehler: ${errorMessage}`
    });
    throw error;
  }
}
async function syncFromManifest(storage, { onProgress, onLog, log, startTime }) {
  onProgress({
    step: "manifest",
    percent: 5,
    message: "Lade Manifest..."
  });
  log("info", "Fetching manifest");
  const manifest = await fetchManifest();
  log("info", "Manifest loaded", {
    version: manifest.version,
    files: manifest.files.length
  });
  const previousHash = await storage.getBvlMeta("lastSyncHash");
  const manifestHash = manifest.hash || manifest.version;
  if (previousHash === manifestHash) {
    onProgress({
      step: "done",
      percent: 100,
      message: "Keine Aktualisierung erforderlich"
    });
    const meta2 = {
      lastSyncHash: manifestHash,
      lastSyncIso: (/* @__PURE__ */ new Date()).toISOString(),
      lastSyncCounts: manifest.counts || manifest.tables || {},
      lastError: null
    };
    await storage.appendBvlSyncLog({
      synced_at: meta2.lastSyncIso,
      ok: 1,
      message: "no-change (manifest)",
      payload_hash: manifestHash
    });
    log("info", "No changes detected, sync complete");
    return { status: "no-change", meta: meta2, manifest };
  }
  onProgress({
    step: "download",
    percent: 10,
    message: "Lade Datenbank..."
  });
  log("info", "Downloading database from manifest");
  const { data, format } = await downloadDatabase(manifest, {
    onProgress: (progress) => {
      if (progress.step === "download") {
        onProgress({
          step: "download",
          percent: 10 + Math.round(progress.percent * 0.5),
          // 10-60%
          message: progress.message
        });
      } else if (progress.step === "decompress") {
        onProgress({
          step: "decompress",
          percent: 60 + Math.round((progress.percent - 70) * 0.2),
          // 60-70%
          message: progress.message
        });
      }
    }
  });
  log("info", `Database downloaded (${format}, ${data.length} bytes)`);
  onProgress({
    step: "import",
    percent: 70,
    message: "Importiere Datenbank..."
  });
  log("info", "Importing database");
  const importStart = Date.now();
  const result = await storage.importBvlSqlite(data, manifest);
  const importDuration = Date.now() - importStart;
  log("info", `Database import complete in ${importDuration}ms`, {
    counts: result.counts
  });
  onProgress({
    step: "verify",
    percent: 95,
    message: "Verifiziere Daten..."
  });
  log("info", "Verifying data");
  const totalDuration = Date.now() - startTime;
  await storage.appendBvlSyncLog({
    synced_at: (/* @__PURE__ */ new Date()).toISOString(),
    ok: 1,
    message: `success (manifest, ${totalDuration}ms)`,
    payload_hash: manifestHash
  });
  onProgress({
    step: "done",
    percent: 100,
    message: "Synchronisation abgeschlossen"
  });
  const meta = {
    lastSyncHash: manifestHash,
    lastSyncIso: (/* @__PURE__ */ new Date()).toISOString(),
    lastSyncCounts: result.counts,
    dataSource: `pflanzenschutzliste-data@${manifest.version}`,
    manifestVersion: manifest.version,
    apiStand: manifest.api_version || manifest.build?.finished_at || null,
    lastError: null
  };
  log("info", `Sync complete in ${totalDuration}ms`, { meta, manifest });
  return { status: "success", meta, manifest };
}
async function syncFromApi(storage, { onProgress, onLog, log, startTime }) {
  const datasets = {};
  const fetchTimes = {};
  let totalProgress = 0;
  const fetchTasks = [
    ...SYNC_ENDPOINTS.map((definition) => ({
      type: definition.category === "core" ? "dataset" : "payload",
      definition
    })),
    ...LOOKUP_CONFIG.map((lookup) => ({ type: "lookup", ...lookup }))
  ];
  const progressPerTask = 70 / fetchTasks.length;
  try {
    for (const task of fetchTasks) {
      const taskStart = Date.now();
      let progressKey;
      let message;
      if (task.type === "dataset" || task.type === "payload") {
        progressKey = task.definition.endpoint;
        message = `Lade ${task.definition.label || task.definition.endpoint}...`;
      } else {
        progressKey = task.progressKey;
        message = `Lade ${task.label}...`;
      }
      onProgress({
        step: `fetch:${progressKey}`,
        percent: Math.round(10 + totalProgress),
        message
      });
      try {
        const items = await fetchCollection(
          task.type === "lookup" ? task.endpoint : task.definition.endpoint,
          {
            params: task.type === "lookup" ? task.params : task.definition?.params,
            onProgress: (progress) => {
              const keyLabel = task.type === "lookup" ? task.progressKey : task.definition?.endpoint;
              log("debug", `Fetch progress for ${keyLabel}`, progress);
            }
          }
        );
        if (task.type === "lookup") {
          const datasetKey = task.key;
          datasets[datasetKey] = items;
          fetchTimes[datasetKey] = Date.now() - taskStart;
          log(
            "info",
            `Fetched ${items.length} items from ${datasetKey} in ${fetchTimes[datasetKey]}ms`
          );
        } else {
          const datasetKey = task.definition.key;
          datasets[datasetKey] = items;
          fetchTimes[datasetKey] = Date.now() - taskStart;
          log(
            "info",
            `Fetched ${items.length} items from ${task.definition.endpoint} in ${fetchTimes[datasetKey]}ms`
          );
        }
        totalProgress += progressPerTask;
      } catch (error) {
        const identifier = task.type === "lookup" ? task.label : task.definition?.label || task.definition?.endpoint;
        log("error", `Failed to fetch ${identifier}`, {
          error: error.message,
          type: error.name,
          status: error.status,
          attempt: error.attempt
        });
        throw new Error(
          `Fehler beim Laden von ${identifier}: ${error.message}`
        );
      }
    }
    onProgress({
      step: "transform",
      percent: 80,
      message: "Verarbeite Daten..."
    });
    log("info", "Transforming data");
    const transformed = transformBvlData(datasets);
    const hashes = await computeDatasetHashes(datasets);
    log("info", "Data transformation complete", {
      counts: Object.entries(transformed).reduce((acc, [key, val]) => {
        acc[key] = Array.isArray(val) ? val.length : 0;
        return acc;
      }, {}),
      hashes
    });
    const previousHash = await storage.getBvlMeta("lastSyncHash");
    if (previousHash === hashes.combined) {
      onProgress({
        step: "done",
        percent: 100,
        message: "Keine Aktualisierung erforderlich"
      });
      const meta2 = {
        lastSyncHash: hashes.combined,
        lastSyncIso: (/* @__PURE__ */ new Date()).toISOString(),
        lastSyncCounts: Object.entries(transformed).reduce(
          (acc, [key, val]) => {
            acc[key] = Array.isArray(val) ? val.length : 0;
            return acc;
          },
          {}
        ),
        lastError: null
      };
      await storage.appendBvlSyncLog({
        synced_at: meta2.lastSyncIso,
        ok: 1,
        message: "no-change",
        payload_hash: hashes.combined
      });
      log("info", "No changes detected, sync complete");
      return { status: "no-change", meta: meta2 };
    }
    onProgress({
      step: "write",
      percent: 85,
      message: "Schreibe in Datenbank..."
    });
    log("info", "Writing to database");
    const writeStart = Date.now();
    await storage.importBvlDataset(transformed, {
      hash: hashes.combined,
      fetchTimes
    });
    const writeDuration = Date.now() - writeStart;
    log("info", `Database write complete in ${writeDuration}ms`);
    onProgress({
      step: "verify",
      percent: 95,
      message: "Verifiziere Daten..."
    });
    log("info", "Verifying data");
    const meta = {
      lastSyncHash: hashes.combined,
      lastSyncIso: (/* @__PURE__ */ new Date()).toISOString(),
      lastSyncCounts: Object.entries(transformed).reduce((acc, [key, val]) => {
        acc[key] = Array.isArray(val) ? val.length : 0;
        return acc;
      }, {}),
      lastError: null
    };
    for (const [key, value] of Object.entries(meta)) {
      await storage.setBvlMeta(
        key,
        typeof value === "object" ? JSON.stringify(value) : value
      );
    }
    const totalDuration = Date.now() - startTime;
    await storage.appendBvlSyncLog({
      synced_at: meta.lastSyncIso,
      ok: 1,
      message: `success (${totalDuration}ms)`,
      payload_hash: hashes.combined
    });
    onProgress({
      step: "done",
      percent: 100,
      message: "Synchronisation abgeschlossen"
    });
    log("info", `Sync complete in ${totalDuration}ms`, { meta });
    return { status: "success", meta };
  } catch (error) {
    const errorMessage = error.message || "Unbekannter Fehler";
    log("error", "API sync failed", {
      error: errorMessage,
      stack: error.stack
    });
    throw error;
  }
}
function parseNullableNumber(value) {
  if (value === null || value === void 0) {
    return null;
  }
  if (typeof value === "string") {
    let normalized = value.replace(/\s+/g, "");
    if (normalized === "") {
      return null;
    }
    if (normalized.includes(",")) {
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    }
    if (normalized === "") {
      return null;
    }
    const parsed = Number(normalized);
    return Number.isNaN(parsed) ? null : parsed;
  }
  if (typeof value === "number") {
    return Number.isNaN(value) ? null : value;
  }
  return null;
}
function coalesceValue(...candidates) {
  for (const value of candidates) {
    if (value === void 0 || value === null) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    return value;
  }
  return null;
}
function transformBvlData(datasets) {
  const result = {
    mittel: [],
    awg: [],
    awg_kultur: [],
    awg_schadorg: [],
    awg_aufwand: [],
    awg_wartezeit: [],
    culturesLookup: [],
    pestsLookup: [],
    apiPayloads: [],
    payloadCounts: {}
  };
  const endpointLookup = new Map(
    SYNC_ENDPOINTS.map((definition) => [definition.key, definition])
  );
  const pushPayload = (definition, item) => {
    if (!definition) {
      return;
    }
    const primaryRef = definition.primaryRefField ? extractFieldValue(item, definition.primaryRefField) : null;
    const secondaryRef = definition.secondaryRefField ? extractFieldValue(item, definition.secondaryRefField) : null;
    const tertiaryRef = definition.tertiaryRefField ? extractFieldValue(item, definition.tertiaryRefField) : null;
    result.apiPayloads.push({
      endpoint: definition.endpoint,
      key: definition.key,
      primary_ref: primaryRef,
      secondary_ref: secondaryRef,
      tertiary_ref: tertiaryRef,
      payload_json: JSON.stringify(item)
    });
    result.payloadCounts[definition.key] = (result.payloadCounts[definition.key] || 0) + 1;
  };
  if (datasets.mittel) {
    result.mittel = datasets.mittel.map((item) => ({
      kennr: item.kennr || "",
      name: item.mittelname || "",
      formulierung: item.formulierung || "",
      zul_erstmalig: item.zul_erstmalig || null,
      zul_ende: item.zul_ende || null,
      geringes_risiko: item.geringes_risiko === "J" ? 1 : 0,
      payload_json: JSON.stringify(item)
    }));
  }
  if (datasets.awg) {
    result.awg = datasets.awg.map((item) => ({
      awg_id: item.awg_id || "",
      kennr: item.kennr || "",
      status_json: JSON.stringify({
        status: item.status || "",
        wachstumsstadium: item.wachstumsstadium || ""
      }),
      zulassungsende: item.zulassungsende || null
    }));
  }
  if (datasets.awg_kultur) {
    result.awg_kultur = datasets.awg_kultur.map((item) => ({
      awg_id: item.awg_id || "",
      kultur: item.kultur || "",
      ausgenommen: item.ausgenommen === "J" ? 1 : 0,
      sortier_nr: parseInt(item.sortier_nr) || 0
    }));
  }
  if (datasets.awg_schadorg) {
    result.awg_schadorg = datasets.awg_schadorg.map((item) => ({
      awg_id: item.awg_id || "",
      schadorg: item.schadorg || "",
      ausgenommen: item.ausgenommen === "J" ? 1 : 0,
      sortier_nr: parseInt(item.sortier_nr) || 0
    }));
  }
  if (datasets.awg_aufwand) {
    result.awg_aufwand = datasets.awg_aufwand.map((item) => ({
      awg_id: item.awg_id || "",
      aufwand_bedingung: item.aufwandbedingung || "",
      sortier_nr: parseInt(item.sortier_nr) || 0,
      mittel_menge: parseNullableNumber(
        coalesceValue(
          item.max_aufwandmenge,
          item.m_aufwand,
          item.m_aufwand_bis,
          item.m_aufwand_von,
          item.aufwandmenge
        )
      ),
      mittel_einheit: coalesceValue(
        item.aufwandmenge_einheit,
        item.m_aufwand_einheit,
        item.m_aufwand_bis_einheit,
        item.m_aufwand_von_einheit
      ) || null,
      wasser_menge: parseNullableNumber(
        coalesceValue(
          item.wassermenge,
          item.w_aufwand_bis,
          item.w_aufwand_von,
          item.wasseraufwand
        )
      ),
      wasser_einheit: coalesceValue(
        item.wassermenge_einheit,
        item.w_aufwand_einheit,
        item.wasseraufwand_einheit,
        item.w_aufwand_von_einheit,
        item.w_aufwand_bis_einheit
      ) || null,
      payload_json: JSON.stringify(item)
    }));
  }
  if (datasets.awg_wartezeit) {
    result.awg_wartezeit = datasets.awg_wartezeit.map((item) => ({
      awg_wartezeit_nr: parseInt(item.awg_wartezeit_nr) || 0,
      awg_id: item.awg_id || "",
      kultur: item.kultur || "",
      sortier_nr: parseInt(item.sortier_nr) || 0,
      tage: parseInt(item.wartezeit_tage) || null,
      bemerkung_kode: item.bemerkung_kode || null,
      anwendungsbereich: item.anwendungsbereich || null,
      erlaeuterung: item.erlaeuterung || null,
      payload_json: JSON.stringify(item)
    }));
  }
  if (datasets.lookupCultures) {
    result.culturesLookup = datasets.lookupCultures.filter(
      (item) => item.sprache ? item.sprache.toUpperCase() === "DE" : true
    ).map((item) => ({
      code: item.kode || "",
      label: item.kodetext || item.kode || ""
    }));
  }
  if (datasets.lookupPests) {
    result.pestsLookup = datasets.lookupPests.filter(
      (item) => item.sprache ? item.sprache.toUpperCase() === "DE" : true
    ).map((item) => ({
      code: item.kode || "",
      label: item.kodetext || item.kode || ""
    }));
  }
  for (const definition of SYNC_ENDPOINTS) {
    if (definition.category !== "payload") {
      continue;
    }
    const items = datasets[definition.key];
    if (!Array.isArray(items) || items.length === 0) {
      continue;
    }
    for (const item of items) {
      pushPayload(definition, item);
    }
  }
  return result;
}

// src/scripts/features/zulassung/index.ts
var numberFormatter = new Intl.NumberFormat("de-DE", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 3
});
var initialized6 = false;
var container = null;
var services = null;
var isSectionVisible = false;
function safeParseJson(value) {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(String(value));
  } catch (error) {
    console.warn("Failed to parse JSON payload in Zulassung view", error);
    return null;
  }
}
function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === void 0 || value === null) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    return value;
  }
  return null;
}
function coerceNumber(value) {
  if (value === null || value === void 0) {
    return null;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }
    let normalized = trimmed.replace(/\s+/g, "");
    if (normalized.includes(",")) {
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    }
    const parsed = Number(normalized);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}
function formatAmount(value, unit) {
  const trimmedUnit = typeof unit === "string" ? unit.trim() : "";
  const numeric = coerceNumber(value);
  if (numeric !== null) {
    const formatted = numberFormatter.format(numeric);
    return trimmedUnit ? `${formatted} ${trimmedUnit}` : formatted;
  }
  if (value === null || value === void 0) {
    return null;
  }
  const text = String(value).trim();
  if (text === "") {
    return null;
  }
  return trimmedUnit ? `${text} ${trimmedUnit}` : text;
}
function formatDateHtml(value) {
  if (value === null || value === void 0) {
    return null;
  }
  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  const parsed = Date.parse(raw);
  if (Number.isNaN(parsed)) {
    return escapeHtml(raw);
  }
  const formatted = new Date(parsed).toLocaleDateString("de-DE");
  if (formatted === raw) {
    return escapeHtml(formatted);
  }
  return `<span title="${escapeHtml(raw)}">${escapeHtml(formatted)}</span>`;
}
function normalizeText(value) {
  if (value === null || value === void 0) {
    return "";
  }
  return String(value).trim();
}
function formatAddressDetails(adresse) {
  if (!adresse || typeof adresse !== "object") {
    return "";
  }
  const lines = [];
  for (let index = 1; index <= 6; index += 1) {
    const key = `anschrift_${index}`;
    const value = adresse[key];
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) {
        lines.push(escapeHtml(trimmed));
      }
    }
  }
  if (adresse.land) {
    lines.push(escapeHtml(String(adresse.land)));
  }
  const contactParts = [];
  if (adresse.telefon) {
    contactParts.push(`Tel: ${escapeHtml(String(adresse.telefon))}`);
  }
  if (adresse.fax) {
    contactParts.push(`Fax: ${escapeHtml(String(adresse.fax))}`);
  }
  if (adresse.e_mail) {
    const email = String(adresse.e_mail).trim();
    if (email) {
      contactParts.push(
        `E-Mail: <a href="mailto:${encodeURIComponent(email)}" class="text-decoration-none">${escapeHtml(email)}</a>`
      );
    }
  }
  const website = firstNonEmpty(adresse.website, adresse.homepage);
  if (website) {
    const url = String(website).trim();
    if (url) {
      contactParts.push(
        `<a href="${escapeHtml(url)}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right me-1"></i>Website</a>`
      );
    }
  }
  const reference = adresse.__meta && adresse.__meta.primary_ref ? String(adresse.__meta.primary_ref) : null;
  const linesHtml = lines.length ? `<div>${lines.join("<br>")}</div>` : "";
  const contactsHtml = contactParts.length ? `<div>${contactParts.join(" \xB7 ")}</div>` : "";
  const referenceHtml = reference ? `<div>Nr.: ${escapeHtml(reference)}</div>` : "";
  const content = `${linesHtml}${contactsHtml}${referenceHtml}`;
  return content ? `<div class="text-muted small mt-1">${content}</div>` : "";
}
function renderIfVisible() {
  if (isSectionVisible) {
    render();
  }
}
function renderAufwandRow(aufwand) {
  const payload = safeParseJson(aufwand.payload_json) || {};
  const mittelUnit = firstNonEmpty(
    aufwand.mittel_einheit,
    payload.aufwandmenge_einheit,
    payload.m_aufwand_einheit,
    payload.max_aufwandmenge_einheit
  );
  const mittelValue = firstNonEmpty(
    aufwand.mittel_menge,
    payload.m_aufwand,
    payload.max_aufwandmenge,
    payload.m_aufwand_bis,
    payload.m_aufwand_von,
    payload.aufwandmenge
  );
  const mittelFrom = firstNonEmpty(
    payload.m_aufwand_von,
    payload.max_aufwandmenge_von
  );
  const mittelTo = firstNonEmpty(
    payload.m_aufwand_bis,
    payload.max_aufwandmenge_bis
  );
  let mittelDisplay = formatAmount(mittelValue, mittelUnit);
  if (!mittelDisplay && (mittelFrom || mittelTo)) {
    const fromDisplay = formatAmount(mittelFrom, mittelUnit);
    const toDisplay = formatAmount(mittelTo, mittelUnit);
    if (fromDisplay && toDisplay && fromDisplay !== toDisplay) {
      mittelDisplay = `${fromDisplay} \u2013 ${toDisplay}`;
    } else if (fromDisplay) {
      mittelDisplay = fromDisplay;
    } else if (toDisplay) {
      mittelDisplay = toDisplay;
    }
  }
  if (!mittelDisplay) {
    mittelDisplay = "keine Angabe";
  }
  const wasserUnit = firstNonEmpty(
    aufwand.wasser_einheit,
    payload.wassermenge_einheit,
    payload.w_aufwand_einheit,
    payload.w_aufwand_von_einheit,
    payload.w_aufwand_bis_einheit
  );
  const wasserValue = firstNonEmpty(
    aufwand.wasser_menge,
    payload.wassermenge,
    payload.w_aufwand,
    payload.w_aufwand_bis,
    payload.w_aufwand_von,
    payload.wasseraufwand
  );
  const wasserFrom = firstNonEmpty(
    payload.w_aufwand_von,
    payload.wassermenge_von
  );
  const wasserTo = firstNonEmpty(
    payload.w_aufwand_bis,
    payload.wassermenge_bis
  );
  let wasserDisplay = formatAmount(wasserValue, wasserUnit);
  if (!wasserDisplay && (wasserFrom || wasserTo)) {
    const fromDisplay = formatAmount(wasserFrom, wasserUnit);
    const toDisplay = formatAmount(wasserTo, wasserUnit);
    if (fromDisplay && toDisplay && fromDisplay !== toDisplay) {
      wasserDisplay = `${fromDisplay} \u2013 ${toDisplay}`;
    } else if (fromDisplay) {
      wasserDisplay = fromDisplay;
    } else if (toDisplay) {
      wasserDisplay = toDisplay;
    }
  }
  const wasserText = wasserDisplay ? `, Wasser: ${escapeHtml(wasserDisplay)}` : "";
  return `${escapeHtml(
    aufwand.aufwand_bedingung || "Standard"
  )}: Mittel: ${escapeHtml(mittelDisplay)}${wasserText}`;
}
async function performAutoUpdateCheck() {
  if (!services) {
    return;
  }
  try {
    const state2 = services.state.getState();
    const currentHash = state2.zulassung.lastSyncHash;
    if (!currentHash) {
      return;
    }
    const updateCheck = await checkForUpdates(currentHash);
    const checkTime = (/* @__PURE__ */ new Date()).toISOString();
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      autoUpdateAvailable: updateCheck.available,
      autoUpdateVersion: updateCheck.newVersion,
      debug: {
        ...prev.debug,
        lastAutoUpdateCheck: {
          time: checkTime,
          result: updateCheck.available ? `Update verf\xFCgbar: ${updateCheck.newVersion}` : "Keine Updates"
        }
      }
    }));
    if (updateCheck.available) {
      renderIfVisible();
    }
  } catch (error) {
    console.warn("Auto-update check failed:", error);
  }
}
function toggleVisibility(state2) {
  if (!container) {
    return;
  }
  const section = container.querySelector(
    '[data-section="zulassung"]'
  );
  if (!section) {
    return;
  }
  const shouldShow = state2.app.activeSection === "zulassung" && state2.app.hasDatabase;
  section.classList.toggle("d-none", !shouldShow);
  if (shouldShow && !isSectionVisible) {
    isSectionVisible = true;
    render();
  } else if (!shouldShow && isSectionVisible) {
    isSectionVisible = false;
  }
}
async function loadInitialData() {
  if (!services) {
    return;
  }
  try {
    if (!isSupported3 || !isSupported3()) {
      return;
    }
    const [
      lastSync,
      lastSyncCounts,
      dataSource,
      apiStand,
      manifestVersion,
      lastSyncHash,
      manifestJson
    ] = await Promise.all([
      getBvlMeta("lastSyncIso"),
      getBvlMeta("lastSyncCounts"),
      getBvlMeta("dataSource"),
      getBvlMeta("apiStand"),
      getBvlMeta("manifestVersion"),
      getBvlMeta("lastSyncHash"),
      getBvlMeta("manifest")
    ]);
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lastSync: lastSync || null,
      lastResultCounts: lastSyncCounts ? JSON.parse(String(lastSyncCounts)) : null,
      dataSource: dataSource || null,
      apiStand: apiStand || null,
      manifestVersion: manifestVersion || null,
      lastSyncHash: lastSyncHash || null,
      debug: {
        ...prev.debug,
        manifest: manifestJson ? JSON.parse(String(manifestJson)) : null
      }
    }));
    const [cultures, pests] = await Promise.all([
      listBvlCultures(),
      listBvlSchadorg()
    ]);
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lookups: { cultures, pests }
    }));
    renderIfVisible();
  } catch (error) {
    console.error("Failed to load initial Zulassung data:", error);
  }
}
function render() {
  if (!container || !services) {
    return;
  }
  let section = container.querySelector(
    '[data-section="zulassung"]'
  );
  if (!section) {
    section = document.createElement("div");
    section.setAttribute("data-section", "zulassung");
    section.className = "section-content";
    container.appendChild(section);
  }
  const state2 = services.state.getState();
  const zulassungState = state2.zulassung;
  section.innerHTML = `
    <div class="container py-4">
      <h2>BVL Zulassungsdaten</h2>
      ${renderStatusSection(zulassungState)}
      ${renderSyncSection(zulassungState)}
      ${renderFilterSection(zulassungState)}
      ${renderResultsSection(zulassungState)}
      ${renderDebugSection(zulassungState)}
    </div>
  `;
  attachEventHandlers(section);
}
function renderStatusSection(zulassungState) {
  if (!zulassungState.lastSync) {
    return `
      <div class="alert alert-info mb-3">
        <i class="bi bi-info-circle-fill me-2"></i>
        <strong>Keine Daten vorhanden.</strong> Bitte f\xFChren Sie eine Synchronisation durch, um BVL-Daten zu laden.
      </div>
    `;
  }
  const lastSyncDate = new Date(zulassungState.lastSync).toLocaleString(
    "de-DE"
  );
  const counts = zulassungState.lastResultCounts || {};
  const dataSource = zulassungState.dataSource || "BVL API";
  const apiStand = zulassungState.apiStand || null;
  const manifestVersion = zulassungState.manifestVersion || null;
  const lastSyncHash = zulassungState.lastSyncHash || null;
  const totalMittel = counts.mittel || counts.bvl_mittel || 0;
  return `
    <div class="alert alert-success mb-3">
      <div class="d-flex justify-content-between align-items-start">
        <div>
          <i class="bi bi-check-circle-fill me-2"></i>
          <strong>Letzte Synchronisation:</strong> ${escapeHtml(lastSyncDate)}<br>
          <strong>Datenquelle:</strong> ${escapeHtml(dataSource)}<br>
          ${manifestVersion ? `<strong>Version:</strong> ${escapeHtml(manifestVersion)}<br>` : ""}
          ${apiStand ? `<strong>API-Stand:</strong> ${escapeHtml(apiStand)}<br>` : ""}
          ${lastSyncHash ? `<small class="text-muted">Hash: ${escapeHtml(lastSyncHash.substring(0, 12))}...</small><br>` : ""}
          <small class="mt-1 d-block">
            <i class="bi bi-database me-1"></i>
            Mittel: ${totalMittel}, Anwendungen: ${counts.awg || counts.bvl_awg || 0}, Kulturen: ${counts.awg_kultur || counts.bvl_awg_kultur || 0}, Schadorganismen: ${counts.awg_schadorg || counts.bvl_awg_schadorg || 0}
          </small>
        </div>
      </div>
    </div>
  `;
}
function renderSyncSection(zulassungState) {
  const isBusy = zulassungState.busy;
  const progress = zulassungState.progress;
  const error = zulassungState.error;
  const stepInfo = {
    manifest: {
      icon: "bi-cloud-download",
      color: "bg-info",
      label: "Manifest"
    },
    download: {
      icon: "bi-cloud-arrow-down",
      color: "bg-info",
      label: "Download"
    },
    decompress: { icon: "bi-archive", color: "bg-primary", label: "Entpacken" },
    import: { icon: "bi-cpu", color: "bg-warning", label: "Import" },
    verify: { icon: "bi-check2", color: "bg-success", label: "Verifizierung" },
    done: {
      icon: "bi-check-circle-fill",
      color: "bg-success",
      label: "Fertig"
    }
  };
  const currentStep = progress.step ? stepInfo[progress.step] || stepInfo.done : null;
  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title"><i class="bi bi-arrow-repeat me-2"></i>Synchronisation</h5>
        ${zulassungState.autoUpdateAvailable ? `
          <div class="alert alert-warning d-flex align-items-center" role="alert">
            <i class="bi bi-exclamation-triangle-fill me-2"></i>
            <div class="flex-grow-1">
              <strong>Neue Daten verf\xFCgbar!</strong><br>
              <small>Version ${escapeHtml(zulassungState.autoUpdateVersion || "unbekannt")} ist verf\xFCgbar.</small>
            </div>
            <button class="btn btn-warning btn-sm ms-2" id="btn-apply-update">
              <i class="bi bi-download me-1"></i>Jetzt aktualisieren
            </button>
          </div>
        ` : ""}
        <button id="btn-sync" class="btn btn-primary" ${isBusy ? "disabled" : ""}>
          ${isBusy ? `<span class="spinner-border spinner-border-sm me-2"></span><i class="${currentStep?.icon || "bi-arrow-repeat"} me-1"></i>` : '<i class="bi bi-arrow-repeat me-1"></i>'}
          ${isBusy ? "Synchronisiere..." : "Daten aktualisieren"}
        </button>
        ${progress.step && isBusy ? `
          <div class="mt-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <small class="text-muted">
                <i class="${currentStep?.icon || "bi-arrow-repeat"} me-1"></i>
                ${escapeHtml(currentStep?.label || "Verarbeite")}: ${escapeHtml(progress.message)}
              </small>
              <small class="text-muted">${progress.percent}%</small>
            </div>
            <div class="progress" style="height: 20px;" role="progressbar" aria-valuenow="${progress.percent}" aria-valuemin="0" aria-valuemax="100" title="${escapeHtml(progress.message)}">
              <div class="progress-bar progress-bar-striped progress-bar-animated ${currentStep?.color || "bg-primary"}" style="width: ${progress.percent}%">
                ${progress.percent}%
              </div>
            </div>
          </div>
        ` : ""}
        ${error ? `
          <div class="alert alert-danger mt-3 d-flex align-items-start">
            <i class="bi bi-exclamation-triangle-fill me-2 mt-1"></i>
            <div class="flex-grow-1">
              <strong>Fehler:</strong> ${escapeHtml(error)}
            </div>
            <button class="btn btn-sm btn-outline-danger ms-2" id="btn-show-debug">
              <i class="bi bi-bug me-1"></i>Debug anzeigen
            </button>
          </div>
        ` : ""}
      </div>
    </div>
  `;
}
function renderFilterSection(zulassungState) {
  const { filters, lookups, busy } = zulassungState;
  const cultures = Array.isArray(lookups.cultures) ? lookups.cultures : [];
  const pests = Array.isArray(lookups.pests) ? lookups.pests : [];
  return `
    <div class="card mb-3 filter-section">
      <div class="card-body">
        <h5 class="card-title"><i class="bi bi-funnel me-2"></i>Filter</h5>
        <div class="row g-3">
          <div class="col-12">
            <label for="filter-text" class="form-label">Schnellsuche</label>
            <input type="search" id="filter-text" class="form-control" placeholder="Mittel, Kultur- oder Schaderreger-Name" value="${escapeHtml(filters.text || "")}">
            <small class="form-text text-muted">Durchsucht Mittelname, Kennnummer sowie Klartexte der Kulturen und Schadorganismen.</small>
          </div>
          <div class="col-md-4">
            <label for="filter-culture" class="form-label">
              <i class="bi bi-flower1 me-1"></i>Kultur
            </label>
            <select id="filter-culture" class="form-select">
              <option value="">Alle Kulturen</option>
              ${cultures.map(
    (culture) => `<option value="${escapeHtml(culture.code)}" ${filters.culture === culture.code ? "selected" : ""}>${escapeHtml(culture.label || culture.code)} (${escapeHtml(culture.code)})</option>`
  ).join("")}
            </select>
          </div>
          <div class="col-md-4">
            <label for="filter-pest" class="form-label">
              <i class="bi bi-bug me-1"></i>Schadorganismus
            </label>
            <select id="filter-pest" class="form-select">
              <option value="">Alle Schadorganismen</option>
              ${pests.map(
    (pest) => `<option value="${escapeHtml(pest.code)}" ${filters.pest === pest.code ? "selected" : ""}>${escapeHtml(pest.label || pest.code)} (${escapeHtml(pest.code)})</option>`
  ).join("")}
            </select>
          </div>
          <div class="col-md-4">
            <label class="form-label d-block">&nbsp;</label>
            <div class="form-check">
              <input class="form-check-input" type="checkbox" id="filter-expired" ${filters.includeExpired ? "checked" : ""}>
              <label class="form-check-label" for="filter-expired">
                <i class="bi bi-clock-history me-1"></i>
                Abgelaufene Zulassungen einschlie\xDFen
              </label>
            </div>
          </div>
        </div>
        <div class="mt-3">
          <button id="btn-search" class="btn btn-primary" ${busy ? "disabled" : ""}>
            <i class="bi bi-search me-1"></i>Suchen
          </button>
          <button id="btn-clear-filters" class="btn btn-secondary ms-2">
            <i class="bi bi-x-circle me-1"></i>Filter zur\xFCcksetzen
          </button>
        </div>
      </div>
    </div>
  `;
}
function renderResultsSection(zulassungState) {
  const { results } = zulassungState;
  if (!Array.isArray(results) || results.length === 0) {
    return `
      <div class="card mb-3">
        <div class="card-body">
          <h5 class="card-title">Ergebnisse</h5>
          <p class="text-muted">Keine Ergebnisse. Bitte w\xE4hlen Sie Filter aus und klicken Sie auf "Suchen".</p>
        </div>
      </div>
    `;
  }
  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title">Ergebnisse (${results.length})</h5>
        <div class="list-group">
          ${results.map((result) => renderResultItem(result)).join("")}
        </div>
      </div>
    </div>
  `;
}
function renderResultItem(result) {
  const status = result.status_json ? safeParseJson(result.status_json) || {} : {};
  return `
    <div class="list-group-item">
      <div class="d-flex w-100 justify-content-between align-items-start">
        <h6 class="mb-1">${escapeHtml(result.name)}</h6>
        <small class="text-muted">AWG: ${escapeHtml(result.awg_id || "-")}</small>
      </div>
      <div class="mb-2">
        <strong>Zulassungsnummer:</strong> ${escapeHtml(result.kennr || "-")}<br>
        <strong>Formulierung:</strong> ${escapeHtml(result.formulierung || "-")}<br>
        <strong>Status:</strong> ${escapeHtml(status.status || "-")}
      </div>
      <div class="mb-2">
        ${result.geringes_risiko ? '<span class="badge bg-success me-1"><i class="bi bi-shield-check me-1"></i>Geringes Risiko</span>' : ""}
        ${result.zul_ende ? `<span class="badge bg-warning text-dark me-1"><i class="bi bi-calendar-event me-1"></i>G\xFCltig bis: ${escapeHtml(result.zul_ende)}</span>` : ""}
        ${result.zulassungsende ? `<span class="badge bg-info text-dark me-1"><i class="bi bi-calendar-range me-1"></i>Anwendung g\xFCltig bis: ${escapeHtml(result.zulassungsende)}</span>` : ""}
      </div>
      ${renderResultWirkstoffe(result)}
      ${renderResultWirkstoffGehalt(result)}
      ${renderResultZusatzstoffe(result)}
      ${renderResultStaerkung(result)}
      ${renderResultAntraege(result)}
      ${renderResultHinweise(result)}
      ${renderResultGefahrhinweise(result)}
      ${renderResultGefahrensymbole(result)}
      ${renderResultSicherheitshinweise(result)}
      ${renderResultSignalwoerter(result)}
      ${renderResultParallelimporte(result)}
      ${renderResultVertrieb(result)}
      ${renderResultZusatzstoffVertrieb(result)}
      ${renderResultStaerkungVertrieb(result)}
      ${renderResultKulturen(result)}
      ${renderResultSchadorganismen(result)}
      ${renderResultAufwaende(result)}
      ${renderResultAuflagen(result)}
      ${renderResultAwgPartner(result)}
      ${renderResultAwgPartnerAufwand(result)}
      ${renderResultAwgBemerkungen(result)}
      ${renderResultAwgVerwendungszwecke(result)}
      ${renderResultWartezeiten(result)}
      ${renderResultAwgWartezeitAusnahmen(result)}
      ${renderResultAwgZeitpunkte(result)}
      ${renderResultAwgZulassung(result)}
    </div>
  `;
}
function renderResultWirkstoffe(result) {
  if (!Array.isArray(result.wirkstoffe) || result.wirkstoffe.length === 0) {
    return "";
  }
  const list = result.wirkstoffe.map((entry) => {
    const gehalt = coerceNumber(entry.gehalt);
    const gehaltStr = gehalt !== null ? numberFormatter.format(gehalt) : String(entry.gehalt || "");
    return `
        <li>
          ${escapeHtml(entry.wirkstoff_name || entry.wirkstoff || "-")}
          ${entry.gehalt ? ` - ${escapeHtml(gehaltStr)} ${escapeHtml(entry.einheit || "")}` : ""}
        </li>
      `;
  }).join("");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-droplet me-1"></i>Wirkstoffe:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultVertrieb(result) {
  if (!Array.isArray(result.vertrieb) || result.vertrieb.length === 0) {
    return "";
  }
  const dedupe = /* @__PURE__ */ new Set();
  const entries = result.vertrieb.map((entry) => {
    const adresse = entry.adresse || null;
    const keyCandidate = adresse && adresse.__meta && adresse.__meta.primary_ref || entry.adresse_nr || entry.vertriebsfirma_nr || entry.hersteller_nr || entry.vertriebsfirma || null;
    const key = keyCandidate ? String(keyCandidate) : JSON.stringify(entry);
    if (dedupe.has(key)) {
      return null;
    }
    dedupe.add(key);
    const displayName = firstNonEmpty(
      entry.hersteller_name,
      entry.hersteller,
      entry.firmenname,
      entry.firma,
      entry.vertriebsfirma_name,
      entry.vertriebsfirma,
      adresse?.firmenname,
      adresse?.firma
    ) || "-";
    const websiteSource = firstNonEmpty(
      entry.website,
      adresse?.website,
      adresse?.homepage
    );
    const websiteLink = websiteSource && String(websiteSource).trim() ? ` <a href="${escapeHtml(String(websiteSource).trim())}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right"></i></a>` : "";
    const addressDetails = formatAddressDetails(adresse);
    const fallbackReference = !addressDetails && keyCandidate ? `<div class="text-muted small mt-1">Nr.: ${escapeHtml(String(keyCandidate))}</div>` : "";
    return `<div class="mb-2"><div class="small fw-semibold">${escapeHtml(displayName)}${websiteLink}</div>${addressDetails || fallbackReference}</div>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!entries) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-building me-1"></i>Hersteller/Vertrieb:</strong>
      <div>${entries}</div>
    </div>
  `;
}
function renderResultGefahrhinweise(result) {
  if (!Array.isArray(result.gefahrhinweise) || result.gefahrhinweise.length === 0) {
    return "";
  }
  const badges = result.gefahrhinweise.map((hint) => {
    const code = hint.hinweis_kode || hint.h_code || hint.h_saetze || "";
    const text = hint.hinweis_text || hint.text || "";
    return `
        <span class="badge bg-danger" title="${escapeHtml(text)}" data-bs-toggle="tooltip">
          ${escapeHtml(code)}${text ? ' <i class="bi bi-info-circle-fill ms-1"></i>' : ""}
        </span>
      `;
  }).join("");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-exclamation-triangle me-1"></i>Gefahrenhinweise:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultWirkstoffGehalt(result) {
  if (!Array.isArray(result.wirkstoff_gehalt) || result.wirkstoff_gehalt.length === 0) {
    return "";
  }
  const entries = result.wirkstoff_gehalt.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const wirknr = normalizeText(
      entry.wirknr ?? entry.wirkstoffnr ?? entry.wirk_nr
    );
    const variant = normalizeText(entry.wirkvar ?? entry.variante);
    const gehaltPrim = firstNonEmpty(
      entry.gehalt_rein_grundstruktur,
      entry.gehalt_rein,
      entry.gehalt
    );
    const gehaltEinheit = firstNonEmpty(
      entry.gehalt_einheit,
      entry.einheit,
      entry.gehalt_einheit
    );
    const gehalt = gehaltPrim !== null ? formatAmount(gehaltPrim, gehaltEinheit) : null;
    const bio = formatAmount(entry.gehalt_bio, entry.gehalt_bio_einheit);
    const detailParts = [];
    if (gehalt) {
      detailParts.push(`Gehalt: ${escapeHtml(gehalt)}`);
    }
    if (bio) {
      detailParts.push(`Bio: ${escapeHtml(bio)}`);
    }
    if (variant) {
      detailParts.push(`Variante: ${escapeHtml(variant)}`);
    }
    const header = wirknr ? `<span class="fw-semibold">${escapeHtml(wirknr)}</span>` : "";
    if (!header && detailParts.length === 0) {
      return null;
    }
    const details = detailParts.join(" \xB7 ");
    return `<li>${header}${header && details ? " \u2013 " : ""}${details}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!entries) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-beaker me-1"></i>Wirkstoffgehalte:</strong>
      <ul class="small mb-0">${entries}</ul>
    </div>
  `;
}
function renderResultZusatzstoffe(result) {
  if (!Array.isArray(result.zusatzstoffe) || result.zusatzstoffe.length === 0) {
    return "";
  }
  const list = result.zusatzstoffe.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const name = normalizeText(
      entry.mittelname ?? entry.mittel ?? entry.name ?? "-"
    );
    const start = formatDateHtml(entry.genehmigung_am);
    const end = formatDateHtml(entry.genehmigungsende);
    const applicant = normalizeText(
      entry.antragsteller ?? entry.antragsteller_name
    );
    const applicantNr = normalizeText(entry.antragsteller_nr);
    const metaParts = [];
    if (start) {
      metaParts.push(`ab ${start}`);
    }
    if (end) {
      metaParts.push(`bis ${end}`);
    }
    if (applicant) {
      metaParts.push(`Antragsteller: ${escapeHtml(applicant)}`);
    }
    if (applicantNr) {
      metaParts.push(`Nr.: ${escapeHtml(applicantNr)}`);
    }
    return `<li><span class="fw-semibold">${escapeHtml(
      name || "-"
    )}</span>${metaParts.length ? ` \u2013 ${metaParts.join(" \xB7 ")}` : ""}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-puzzle me-1"></i>Zusatzstoffe:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultZusatzstoffVertrieb(result) {
  if (!Array.isArray(result.zusatzstoff_vertrieb) || result.zusatzstoff_vertrieb.length === 0) {
    return "";
  }
  const list = result.zusatzstoff_vertrieb.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const name = normalizeText(
      entry.vertriebsfirma_name ?? entry.vertriebsfirma ?? entry.firma ?? entry.hersteller
    );
    const number = normalizeText(entry.vertriebsfirma_nr ?? entry.nr);
    if (!name && !number) {
      return null;
    }
    const parts = [];
    if (name) {
      parts.push(escapeHtml(name));
    }
    if (number) {
      parts.push(
        `<span class="text-muted">Nr.: ${escapeHtml(number)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-2 me-1"></i>Zusatzstoff-Vertrieb:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultStaerkung(result) {
  if (!Array.isArray(result.staerkung) || result.staerkung.length === 0) {
    return "";
  }
  const list = result.staerkung.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const name = normalizeText(entry.mittelname ?? entry.mittel ?? "-");
    const start = formatDateHtml(entry.genehmigung_am);
    const applicant = normalizeText(entry.antragsteller);
    const applicantNr = normalizeText(entry.antragsteller_nr);
    const metaParts = [];
    if (start) {
      metaParts.push(`Genehmigt am ${start}`);
    }
    if (applicant) {
      metaParts.push(`Antragsteller: ${escapeHtml(applicant)}`);
    }
    if (applicantNr) {
      metaParts.push(`Nr.: ${escapeHtml(applicantNr)}`);
    }
    return `<li><span class="fw-semibold">${escapeHtml(
      name || "-"
    )}</span>${metaParts.length ? ` \u2013 ${metaParts.join(" \xB7 ")}` : ""}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-leaf me-1"></i>St\xE4rkungsmittel:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultStaerkungVertrieb(result) {
  if (!Array.isArray(result.staerkung_vertrieb) || result.staerkung_vertrieb.length === 0) {
    return "";
  }
  const list = result.staerkung_vertrieb.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const company = normalizeText(
      entry.vertriebsfirma_name ?? entry.vertriebsfirma ?? entry.firma ?? entry.hersteller
    );
    const number = normalizeText(entry.vertriebsfirma_nr ?? entry.nr);
    if (!company && !number) {
      return null;
    }
    const parts = [];
    if (company) {
      parts.push(escapeHtml(company));
    }
    if (number) {
      parts.push(
        `<span class="text-muted">Nr.: ${escapeHtml(number)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-2-fill me-1"></i>St\xE4rkungsmittel-Vertrieb:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAntraege(result) {
  if (!Array.isArray(result.antraege) || result.antraege.length === 0) {
    return "";
  }
  const list = result.antraege.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const number = normalizeText(entry.antragnr ?? entry.nr);
    const applicant = normalizeText(
      entry.antragsteller ?? entry.antragsteller_name
    );
    const applicantNr = normalizeText(entry.antragsteller_nr);
    if (!number && !applicant && !applicantNr) {
      return null;
    }
    const parts = [];
    if (number) {
      parts.push(`Antrag ${escapeHtml(number)}`);
    }
    if (applicant) {
      parts.push(`von ${escapeHtml(applicant)}`);
    }
    if (applicantNr) {
      parts.push(
        `<span class="text-muted">Nr.: ${escapeHtml(applicantNr)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-inbox me-1"></i>Antr\xE4ge:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultHinweise(result) {
  if (!Array.isArray(result.hinweise) || result.hinweise.length === 0) {
    return "";
  }
  const hintMap = /* @__PURE__ */ new Map();
  for (const entry of result.hinweise) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const code = normalizeText(entry.hinweis ?? entry.code);
    if (!code) {
      continue;
    }
    const level = normalizeText(entry.ebene ?? result.kennr ?? "-") || "-";
    if (!hintMap.has(level)) {
      hintMap.set(level, []);
    }
    hintMap.get(level).push(code);
  }
  if (!hintMap.size) {
    return "";
  }
  const rows = Array.from(hintMap.entries()).map(([level, codes]) => {
    const uniqueCodes = Array.from(new Set(codes.filter(Boolean)));
    if (!uniqueCodes.length) {
      return null;
    }
    const badges = uniqueCodes.map(
      (code) => `<span class="badge bg-secondary">${escapeHtml(code)}</span>`
    ).join(" ");
    return `<li><span class="fw-semibold">${escapeHtml(
      level
    )}</span>: ${badges}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!rows) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-info-circle me-1"></i>Hinweise:</strong>
      <ul class="small mb-0 d-flex flex-column gap-1">${rows}</ul>
    </div>
  `;
}
function renderResultGefahrensymbole(result) {
  if (!Array.isArray(result.gefahrensymbole) || result.gefahrensymbole.length === 0) {
    return "";
  }
  const unique = Array.from(
    new Set(
      result.gefahrensymbole.map(
        (entry) => entry && typeof entry === "object" ? normalizeText(entry.gefahrensymbol ?? entry.symbol ?? entry.code) : ""
      ).filter((code) => code)
    )
  );
  if (!unique.length) {
    return "";
  }
  const badges = unique.map(
    (code) => `<span class="badge bg-dark text-light border border-light">${escapeHtml(code)}</span>`
  ).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-sign-stop me-1"></i>Gefahrensymbole:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultSicherheitshinweise(result) {
  if (!Array.isArray(result.sicherheitshinweise) || result.sicherheitshinweise.length === 0) {
    return "";
  }
  const unique = Array.from(
    new Set(
      result.sicherheitshinweise.map(
        (entry) => entry && typeof entry === "object" ? normalizeText(
          entry.sicherheitshinweis ?? entry.p_code ?? entry.p_satz ?? entry.code
        ) : ""
      ).filter((code) => code)
    )
  );
  if (!unique.length) {
    return "";
  }
  const badges = unique.map(
    (code) => `<span class="badge bg-warning text-dark">${escapeHtml(code)}</span>`
  ).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-shield-exclamation me-1"></i>Sicherheitshinweise:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultSignalwoerter(result) {
  if (!Array.isArray(result.signalwoerter) || result.signalwoerter.length === 0) {
    return "";
  }
  const unique = Array.from(
    new Set(
      result.signalwoerter.map(
        (entry) => entry && typeof entry === "object" ? normalizeText(entry.signalwort ?? entry.signal_word ?? entry.code) : ""
      ).filter((code) => code)
    )
  );
  if (!unique.length) {
    return "";
  }
  const badges = unique.map(
    (value) => `<span class="badge bg-secondary">${escapeHtml(value)}</span>`
  ).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-signpost me-1"></i>Signalw\xF6rter:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultParallelimporte(result) {
  const sections = [];
  const buildList = (entries, title, variant) => {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    const items = entries.map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }
      const name = normalizeText(
        entry.pi_mittelname ?? entry.mittelname ?? "-"
      );
      const importer = normalizeText(
        entry.importeur_txt ?? entry.importeur_name ?? entry.importeur
      );
      const importerCode = normalizeText(entry.importeur);
      const importerNr = normalizeText(entry.importeur_nr);
      const reference = normalizeText(
        entry.pi_referenz_kennr ?? entry.referenz_kennr
      );
      const kennziffer = normalizeText(
        entry.pi_kennziffer ?? entry.kennziffer
      );
      const status = normalizeText(entry.pi_status ?? entry.status);
      const gueltig = formatDateHtml(entry.gueltig);
      const bescheidNr = normalizeText(
        entry.pi_bescheidnr ?? entry.bescheid_nr
      );
      const bescheidDatum = formatDateHtml(entry.bescheid_datum);
      const bescheinigung = normalizeText(entry.bescheinigung);
      const headline = `<span class="fw-semibold">${escapeHtml(
        name || "-"
      )}</span>`;
      const metaParts = [];
      if (importer) {
        const suffix = importerCode && importerCode !== importer ? ` (${escapeHtml(importerCode)})` : "";
        metaParts.push(`Importeur: ${escapeHtml(importer)}${suffix}`);
      }
      if (importerNr) {
        metaParts.push(`Nr.: ${escapeHtml(importerNr)}`);
      }
      if (reference) {
        metaParts.push(`Referenz: ${escapeHtml(reference)}`);
      }
      if (kennziffer) {
        metaParts.push(`Kennziffer: ${escapeHtml(kennziffer)}`);
      }
      if (status) {
        metaParts.push(`Status: ${escapeHtml(status)}`);
      }
      if (gueltig) {
        const label = variant === "expired" ? "Ausgelaufen am" : "G\xFCltig bis";
        metaParts.push(`${label} ${gueltig}`);
      }
      if (bescheidNr) {
        metaParts.push(`Bescheid ${escapeHtml(bescheidNr)}`);
      }
      if (bescheidDatum) {
        metaParts.push(`vom ${bescheidDatum}`);
      }
      if (bescheinigung) {
        metaParts.push(`Bescheinigung: ${escapeHtml(bescheinigung)}`);
      }
      const meta = metaParts.length > 0 ? `<div class="small text-muted">${metaParts.join(" \xB7 ")}</div>` : "";
      return `<li>${headline}${meta}</li>`;
    }).filter((entry) => Boolean(entry)).join("");
    if (!items) {
      return;
    }
    sections.push(`
      <div class="mt-2">
        <em class="d-block">${escapeHtml(title)}:</em>
        <ul class="small mb-0">${items}</ul>
      </div>
    `);
  };
  buildList(result.parallelimporte_gueltig, "G\xFCltige Parallelimporte", "valid");
  buildList(
    result.parallelimporte_abgelaufen,
    "Abgelaufene Parallelimporte",
    "expired"
  );
  if (!sections.length) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-arrow-left-right me-1"></i>Parallelimporte:</strong>
      ${sections.join("")}
    </div>
  `;
}
function renderResultAuflagen(result) {
  if (!Array.isArray(result.auflagen) || result.auflagen.length === 0) {
    return "";
  }
  const list = result.auflagen.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const code = normalizeText(entry.auflage ?? entry.code);
    const level = normalizeText(entry.ebene);
    const culture = normalizeText(entry.kultur);
    const condition = normalizeText(entry.weitere_bedingung);
    const technique = normalizeText(entry.anwendungstechnik);
    const distance = normalizeText(entry.abstand);
    const reducedDistance = normalizeText(entry.redu_abstand);
    const requirement = normalizeText(entry.anwendbest);
    const metaParts = [];
    if (level) {
      metaParts.push(`Ebene: ${escapeHtml(level)}`);
    }
    if (culture) {
      metaParts.push(`Kultur: ${escapeHtml(culture)}`);
    }
    if (condition) {
      metaParts.push(escapeHtml(condition));
    }
    if (technique) {
      metaParts.push(`Technik: ${escapeHtml(technique)}`);
    }
    if (distance) {
      metaParts.push(`Abstand: ${escapeHtml(distance)}`);
    }
    if (reducedDistance) {
      metaParts.push(`reduz. Abstand: ${escapeHtml(reducedDistance)}`);
    }
    const reductions = Array.isArray(entry.reduzierung) && entry.reduzierung.length > 0 ? entry.reduzierung.map((redu) => {
      if (!redu || typeof redu !== "object") {
        return null;
      }
      const category = normalizeText(redu.kategorie);
      const value = normalizeText(redu.redu_abstand ?? redu.wert);
      const parts = [];
      if (category) {
        parts.push(escapeHtml(category));
      }
      if (value) {
        parts.push(escapeHtml(value));
      }
      if (!parts.length) {
        return null;
      }
      return `<span class="badge bg-light text-dark border">${parts.join(" \u2013 ")}</span>`;
    }).filter((redu) => Boolean(redu)).join(" ") : "";
    const header = code ? `<span class="fw-semibold">${escapeHtml(code)}</span>` : `<span class="fw-semibold">Auflage</span>`;
    const requirementBadge = requirement ? ` <span class="badge bg-secondary">${escapeHtml(requirement)}</span>` : "";
    const metaHtml = metaParts.length > 0 ? `<div class="small text-muted">${metaParts.join(" \xB7 ")}</div>` : "";
    const reductionHtml = reductions ? `<div class="mt-1 d-flex flex-wrap gap-1">${reductions}</div>` : "";
    return `<li>${header}${requirementBadge}${metaHtml}${reductionHtml}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-clipboard-check me-1"></i>Auflagen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgPartner(result) {
  if (!Array.isArray(result.awg_partner) || result.awg_partner.length === 0) {
    return "";
  }
  const list = result.awg_partner.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const partnerKennr = normalizeText(entry.partner_kennr ?? entry.kennr);
    const art = normalizeText(entry.mischung_art ?? entry.art);
    if (!partnerKennr && !art) {
      return null;
    }
    const parts = [];
    if (partnerKennr) {
      parts.push(`Partner: ${escapeHtml(partnerKennr)}`);
    }
    if (art) {
      parts.push(`Art: ${escapeHtml(art)}`);
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-3 me-1"></i>Mischpartner:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgPartnerAufwand(result) {
  if (!Array.isArray(result.awg_partner_aufwand) || result.awg_partner_aufwand.length === 0) {
    return "";
  }
  const list = result.awg_partner_aufwand.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const partnerKennr = normalizeText(entry.partner_kennr ?? entry.kennr);
    const condition = normalizeText(
      entry.aufwandbedingung ?? entry.aufwand_bedingung
    );
    const amount = formatAmount(
      entry.m_aufwand ?? entry.mittel_menge,
      entry.m_aufwand_einheit ?? entry.mittel_einheit
    );
    const parts = [];
    if (partnerKennr) {
      parts.push(`Partner: ${escapeHtml(partnerKennr)}`);
    }
    if (condition) {
      parts.push(`Bedingung: ${escapeHtml(condition)}`);
    }
    if (amount) {
      parts.push(`Menge: ${escapeHtml(amount)}`);
    }
    if (!parts.length) {
      return null;
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-droplet-half me-1"></i>Mischpartner-Aufwand:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgBemerkungen(result) {
  if (!Array.isArray(result.awg_bemerkungen) || result.awg_bemerkungen.length === 0) {
    return "";
  }
  const list = result.awg_bemerkungen.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const note = normalizeText(entry.auflage_bem ?? entry.bemerkung);
    const area = normalizeText(entry.auflage_bereich ?? entry.bereich);
    if (!note && !area) {
      return null;
    }
    const parts = [];
    if (note) {
      parts.push(escapeHtml(note));
    }
    if (area) {
      parts.push(
        `<span class="text-muted">Bereich: ${escapeHtml(area)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-chat-left-text me-1"></i>AWG-Bemerkungen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgVerwendungszwecke(result) {
  if (!Array.isArray(result.awg_verwendungszwecke) || result.awg_verwendungszwecke.length === 0) {
    return "";
  }
  const unique = Array.from(
    new Set(
      result.awg_verwendungszwecke.map(
        (entry) => entry && typeof entry === "object" ? normalizeText(entry.verwendungszweck ?? entry.code) : ""
      ).filter((code) => code)
    )
  );
  if (!unique.length) {
    return "";
  }
  const badges = unique.map((code) => `<span class="badge bg-primary">${escapeHtml(code)}</span>`).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-list-task me-1"></i>Verwendungszwecke:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultAwgWartezeitAusnahmen(result) {
  if (!Array.isArray(result.awg_wartezeit_ausnahmen) || result.awg_wartezeit_ausnahmen.length === 0) {
    return "";
  }
  const labelMap = /* @__PURE__ */ new Map();
  if (Array.isArray(result.kulturen)) {
    for (const entry of result.kulturen) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const code = normalizeText(entry.kultur);
      const label = normalizeText(entry.label);
      if (code && label) {
        labelMap.set(code, label);
      }
    }
  }
  if (Array.isArray(result.wartezeiten)) {
    for (const entry of result.wartezeiten) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const code = normalizeText(entry.kultur);
      const label = normalizeText(entry.kultur_label);
      if (code && label && !labelMap.has(code)) {
        labelMap.set(code, label);
      }
    }
  }
  const list = result.awg_wartezeit_ausnahmen.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const wartezeitNr = normalizeText(entry.awg_wartezeit_nr);
    const kulturCode = normalizeText(entry.kultur);
    if (!wartezeitNr && !kulturCode) {
      return null;
    }
    const displayLabel = kulturCode ? labelMap.get(kulturCode) || kulturCode : "";
    const parts = [];
    if (displayLabel) {
      parts.push(escapeHtml(displayLabel));
    }
    if (wartezeitNr) {
      parts.push(
        `<span class="text-muted">Nr.: ${escapeHtml(wartezeitNr)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-hourglass-split me-1"></i>Wartezeit-Ausnahmen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgZeitpunkte(result) {
  if (!Array.isArray(result.awg_zeitpunkte) || result.awg_zeitpunkte.length === 0) {
    return "";
  }
  const list = result.awg_zeitpunkte.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const zeitpunkt = normalizeText(entry.zeitpunkt ?? entry.code);
    const sortierNr = normalizeText(entry.sortier_nr);
    const operand = normalizeText(entry.operand_zu_vorher ?? entry.operand);
    if (!zeitpunkt && !sortierNr && !operand) {
      return null;
    }
    const parts = [];
    if (zeitpunkt) {
      parts.push(escapeHtml(zeitpunkt));
    }
    if (sortierNr) {
      parts.push(
        `<span class="text-muted">Pos.: ${escapeHtml(sortierNr)}</span>`
      );
    }
    if (operand) {
      parts.push(
        `<span class="text-muted">Operand: ${escapeHtml(operand)}</span>`
      );
    }
    return `<li>${parts.join(" \xB7 ")}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-calendar-week me-1"></i>Anwendungszeitpunkte:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultAwgZulassung(result) {
  if (!Array.isArray(result.awg_zulassung) || result.awg_zulassung.length === 0) {
    return "";
  }
  const list = result.awg_zulassung.map((entry) => {
    if (!entry || typeof entry !== "object") {
      return null;
    }
    const end = formatDateHtml(entry.zul_ende ?? entry.gueltig_bis);
    if (!end) {
      return null;
    }
    return `<li>G\xFCltig bis ${end}</li>`;
  }).filter((entry) => Boolean(entry)).join("");
  if (!list) {
    return "";
  }
  return `
    <div class="mt-2">
      <strong><i class="bi bi-calendar-check me-1"></i>AWG-Zulassung:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultKulturen(result) {
  if (!Array.isArray(result.kulturen) || result.kulturen.length === 0) {
    return "";
  }
  const badges = result.kulturen.map(
    (entry) => `<span class="badge ${entry.ausgenommen ? "bg-danger" : "bg-info"}" title="${escapeHtml(entry.kultur)}">${escapeHtml(
      entry.label || entry.kultur
    )}${entry.ausgenommen ? " (ausgenommen)" : ""}</span>`
  ).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-flower1 me-1"></i>Kulturen:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultSchadorganismen(result) {
  if (!Array.isArray(result.schadorganismen) || result.schadorganismen.length === 0) {
    return "";
  }
  const badges = result.schadorganismen.map(
    (entry) => `<span class="badge ${entry.ausgenommen ? "bg-danger" : "bg-secondary"}" title="${escapeHtml(entry.schadorg)}">${escapeHtml(
      entry.label || entry.schadorg
    )}${entry.ausgenommen ? " (ausgenommen)" : ""}</span>`
  ).join(" ");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-bug me-1"></i>Schadorganismen:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}
function renderResultAufwaende(result) {
  if (!Array.isArray(result.aufwaende) || result.aufwaende.length === 0) {
    return "";
  }
  const list = result.aufwaende.map((entry) => `<li>${renderAufwandRow(entry)}</li>`).join("");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-activity me-1"></i>Aufw\xE4nde:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderResultWartezeiten(result) {
  if (!Array.isArray(result.wartezeiten) || result.wartezeiten.length === 0) {
    return "";
  }
  const list = result.wartezeiten.map((entry) => {
    const anwendungsbereich = entry.anwendungsbereich ? ` (${escapeHtml(entry.anwendungsbereich)})` : "";
    return `
        <li>
          ${escapeHtml(entry.kultur_label || entry.kultur)}: ${escapeHtml(entry.tage || "-")} Tage${anwendungsbereich}
        </li>
      `;
  }).join("");
  return `
    <div class="mt-2">
      <strong><i class="bi bi-clock me-1"></i>Wartezeiten:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}
function renderDebugSection(zulassungState) {
  const { debug, logs } = zulassungState;
  const manifestHtml = debug.manifest ? `
      <div class="mt-3">
        <h6><i class="bi bi-file-code me-1"></i>Manifest</h6>
        <details>
          <summary class="btn btn-sm btn-outline-secondary">JSON anzeigen</summary>
          <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 300px; overflow-y: auto;">${escapeHtml(
    JSON.stringify(debug.manifest, null, 2)
  )}</pre>
        </details>
      </div>
    ` : "";
  const autoUpdateHtml = debug.lastAutoUpdateCheck ? `
      <div class="mt-3">
        <h6><i class="bi bi-clock-history me-1"></i>Letzter Auto-Update-Check</h6>
        <p class="small mb-0">
          <strong>Zeit:</strong> ${escapeHtml(
    new Date(debug.lastAutoUpdateCheck.time).toLocaleString("de-DE")
  )}<br>
          <strong>Ergebnis:</strong> ${escapeHtml(debug.lastAutoUpdateCheck.result || "OK")}
        </p>
      </div>
    ` : "";
  const syncLogHtml = Array.isArray(debug.lastSyncLog) && debug.lastSyncLog.length > 0 ? debug.lastSyncLog.map(
    (log) => `
            <tr>
              <td><small>${escapeHtml(new Date(log.synced_at).toLocaleString("de-DE"))}</small></td>
              <td>${log.ok ? '<span class="badge bg-success"><i class="bi bi-check-lg"></i> OK</span>' : '<span class="badge bg-danger"><i class="bi bi-x-lg"></i> Fehler</span>'}</td>
              <td><small>${escapeHtml(log.message)}</small></td>
            </tr>
          `
  ).join("") : '<tr><td colspan="3" class="text-muted">Keine Logs vorhanden</td></tr>';
  const sessionLogsHtml = logs.length ? logs.slice(-50).map((log) => {
    const badgeClass = log.level === "error" ? "bg-danger" : log.level === "warn" ? "bg-warning" : log.level === "debug" ? "bg-secondary" : "bg-primary";
    return `<div><span class="badge ${badgeClass} me-1">${escapeHtml(log.level.toUpperCase())}</span> ${escapeHtml(log.message)}</div>`;
  }).join("") : "";
  const schemaHtml = debug.schema ? `
      <div class="mt-3">
        <h6><i class="bi bi-diagram-3 me-1"></i>Schema-Informationen</h6>
        <p><strong>User Version:</strong> ${escapeHtml(debug.schema.user_version)}</p>
        <details>
          <summary class="btn btn-sm btn-outline-secondary">Tabellen anzeigen</summary>
          <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 400px; overflow-y: auto;">${escapeHtml(
    JSON.stringify(debug.schema.tables, null, 2)
  )}</pre>
        </details>
      </div>
    ` : "";
  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title">
          <button class="btn btn-sm btn-link text-decoration-none p-0" type="button" data-bs-toggle="collapse" data-bs-target="#debug-panel">
            <i class="bi bi-tools me-2"></i>Debug-Informationen \u25BC
          </button>
        </h5>
        <div class="collapse" id="debug-panel">
          ${manifestHtml}
          ${autoUpdateHtml}
          <div class="mt-3">
            <h6><i class="bi bi-list-ul me-1"></i>Sync-Logs (letzte 10)</h6>
            <div class="table-responsive">
              <table class="table table-sm">
                <thead>
                  <tr>
                    <th>Zeit</th>
                    <th>Status</th>
                    <th>Nachricht</th>
                  </tr>
                </thead>
                <tbody>${syncLogHtml}</tbody>
              </table>
            </div>
          </div>
          ${sessionLogsHtml ? `
            <div class="mt-3">
              <h6><i class="bi bi-terminal me-1"></i>Aktuelle Session Logs</h6>
              <div class="bg-dark text-light p-2" style="max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px;">
                ${sessionLogsHtml}
              </div>
            </div>
          ` : ""}
          ${schemaHtml}
        </div>
      </div>
    </div>
  `;
}
function attachEventHandlers(section) {
  const btnSync = section.querySelector("#btn-sync");
  const btnSearch = section.querySelector("#btn-search");
  const btnClearFilters = section.querySelector("#btn-clear-filters");
  const btnShowDebug = section.querySelector("#btn-show-debug");
  if (btnSync) {
    btnSync.addEventListener("click", handleSync);
  }
  if (btnSearch) {
    btnSearch.addEventListener("click", handleSearch);
  }
  if (btnClearFilters) {
    btnClearFilters.addEventListener("click", handleClearFilters);
  }
  if (btnShowDebug) {
    btnShowDebug.addEventListener("click", () => {
      const debugPanel = section.querySelector("#debug-panel");
      if (debugPanel && typeof bootstrap !== "undefined" && bootstrap?.Collapse) {
        new bootstrap.Collapse(debugPanel, { toggle: true });
      }
    });
  }
  const filterCulture = section.querySelector("#filter-culture");
  const filterPest = section.querySelector("#filter-pest");
  const filterText = section.querySelector("#filter-text");
  const filterExpired = section.querySelector("#filter-expired");
  const btnApplyUpdate = section.querySelector("#btn-apply-update");
  if (filterCulture && services) {
    filterCulture.addEventListener("change", (event) => {
      const target = event.target;
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, culture: target.value || null }
      }));
    });
  }
  if (filterPest && services) {
    filterPest.addEventListener("change", (event) => {
      const target = event.target;
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, pest: target.value || null }
      }));
    });
  }
  if (filterText && services) {
    filterText.addEventListener("input", (event) => {
      const target = event.target;
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, text: target.value }
      }));
    });
    filterText.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && services && !services.state.getState().zulassung.busy) {
        handleSearch();
      }
    });
  }
  if (filterExpired && services) {
    filterExpired.addEventListener("change", (event) => {
      const target = event.target;
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, includeExpired: target.checked }
      }));
    });
  }
  if (btnApplyUpdate) {
    btnApplyUpdate.addEventListener("click", () => {
      handleSync();
    });
  }
}
async function handleSync() {
  if (!services) {
    return;
  }
  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    busy: true,
    error: null,
    logs: [],
    progress: { step: "start", percent: 0, message: "Starte..." }
  }));
  render();
  try {
    const result = await syncBvlData(sqlite_exports, {
      onProgress: (progress) => {
        services.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          progress
        }));
        render();
      },
      onLog: (log) => {
        services.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          logs: [...prev.logs, log]
        }));
      }
    });
    const meta = result.meta;
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      lastSync: meta.lastSyncIso ?? prev.lastSync,
      lastResultCounts: meta.lastSyncCounts ?? prev.lastResultCounts,
      dataSource: meta.dataSource ?? prev.dataSource,
      apiStand: meta.apiStand ?? prev.apiStand,
      manifestVersion: meta.manifestVersion ?? prev.manifestVersion,
      lastSyncHash: meta.lastSyncHash ?? prev.lastSyncHash,
      progress: { step: null, percent: 0, message: "" },
      autoUpdateAvailable: false,
      autoUpdateVersion: null
    }));
    await loadInitialData();
    const [syncLog, schema] = await Promise.all([
      listBvlSyncLog({ limit: 10 }),
      diagnoseBvlSchema()
    ]);
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      debug: {
        ...prev.debug,
        schema,
        lastSyncLog: syncLog
      }
    }));
    render();
  } catch (error) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error?.message || "Unbekannter Fehler",
      progress: { step: null, percent: 0, message: "" }
    }));
    render();
  }
}
async function handleSearch() {
  if (!services) {
    return;
  }
  const state2 = services.state.getState();
  const { filters } = state2.zulassung;
  const normalizedFilters = {
    ...filters,
    text: filters.text ? filters.text.trim() : ""
  };
  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    busy: true
  }));
  render();
  try {
    const results = await queryZulassung(normalizedFilters);
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      filters: { ...prev.filters, text: normalizedFilters.text },
      results
    }));
    render();
  } catch (error) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error?.message || "Unbekannter Fehler"
    }));
    render();
  }
}
function handleClearFilters() {
  if (!services) {
    return;
  }
  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    filters: {
      culture: null,
      pest: null,
      text: "",
      includeExpired: false
    },
    results: []
  }));
  render();
}
function initZulassung(target, providedServices) {
  if (!target || initialized6) {
    return;
  }
  container = target;
  services = providedServices;
  initialized6 = true;
  render();
  services.state.subscribe((state2) => {
    toggleVisibility(state2);
  });
  services.events.subscribe?.("database:connected", async () => {
    await loadInitialData();
    setTimeout(() => {
      void performAutoUpdateCheck();
    }, 2e3);
  });
  const initialState = services.state.getState();
  toggleVisibility(initialState);
  if (initialState.app.hasDatabase) {
    void (async () => {
      await loadInitialData();
      setTimeout(() => {
        void performAutoUpdateCheck();
      }, 2e3);
    })();
  }
}

// src/scripts/pages/indexClient.ts
if (typeof document !== "undefined") {
  let initIndex = function() {
    const services2 = {
      state: {
        getState,
        updateSlice,
        subscribe: subscribeState
      },
      events: {
        emit,
        subscribe
      }
    };
    const startupRegion = document.querySelector('[data-region="startup"]');
    const shellRegion = document.querySelector('[data-region="shell"]');
    const mainRegion = document.querySelector('[data-region="main"]');
    const footerRegion = document.querySelector('[data-region="footer"]');
    initStartup(startupRegion, services2);
    const calcContainer = document.querySelector(
      '[data-feature="calculation"]'
    );
    initCalculation(calcContainer, services2);
    const historyContainer = document.querySelector('[data-feature="history"]');
    initHistory(historyContainer, services2);
    const settingsContainer = document.querySelector(
      '[data-feature="settings"]'
    );
    initSettings(settingsContainer, services2);
    const reportingContainer = document.querySelector(
      '[data-feature="report"]'
    );
    initReporting(reportingContainer, services2);
    const zulassungContainer = document.querySelector(
      '[data-feature="zulassung"]'
    );
    initZulassung(zulassungContainer, services2);
    const toggleRegions = (state2) => {
      const hasDatabase = Boolean(state2.app?.hasDatabase);
      if (startupRegion instanceof HTMLElement) {
        startupRegion.classList.toggle("d-none", hasDatabase);
      }
      if (shellRegion instanceof HTMLElement) {
        shellRegion.classList.toggle("d-none", !hasDatabase);
      }
      if (mainRegion instanceof HTMLElement) {
        mainRegion.classList.toggle("d-none", !hasDatabase);
      }
      if (footerRegion instanceof HTMLElement) {
        footerRegion.classList.toggle("d-none", !hasDatabase);
      }
      if (hasDatabase) {
        const activeSection = state2.app?.activeSection ?? "calc";
        document.querySelectorAll(".content-section").forEach((element) => {
          element.style.display = "none";
        });
        const target = document.getElementById(`section-${activeSection}`);
        if (target instanceof HTMLElement) {
          target.style.display = "block";
        }
      }
    };
    toggleRegions(services2.state.getState());
    subscribeState(toggleRegions);
    subscribe("app:sectionChanged", (section) => {
      document.querySelectorAll(".content-section").forEach((element) => {
        element.style.display = "none";
      });
      const targetSection = document.getElementById(`section-${section}`);
      if (targetSection instanceof HTMLElement) {
        targetSection.style.display = "block";
      }
    });
  };
  initIndex2 = initIndex;
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initIndex, { once: true });
  } else {
    initIndex();
  }
}
var initIndex2;
