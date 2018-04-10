package com.hzw.monitor.mysqlbinlog.utils;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Calendar;

import com.hzw.monitor.mysqlbinlog.event.data.TableMapEventData;
import com.hzw.monitor.mysqlbinlog.type.ColumnType;

import io.netty.buffer.ByteBuf;

public class ByteUtils {
	// 处理各种字符串读取的操作,都是从当前位置开始处理,根据大端小端来处理
	// 专门用于处理字节流,默认都采用小端模式
	// 本类需要优化，做字节边界安全控制，TODO
	// 下面主要用作读

	private static final int DIG_PER_DEC = 9;
	private static final int[] DIG_TO_BYTES = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

	public static int verify(byte value) {
		if (value < 0) {
			return (int) (256 + value);
		}
		return value;
	}

	private static final ThreadLocal<SimpleDateFormat> localDateFormat = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};
	
	//private static java.util.Date deserializeDatetimeV2(int meta, ByteBuf msg) throws IOException {
	  private static String         deserializeDatetimeV2(int meta, ByteBuf msg) throws IOException {
		/*
		 * in big endian:
		 * 
		 * 1 bit sign (1= non-negative, 0= negative) 17 bits year*13+month (year
		 * 0-9999, month 0-12) 5 bits day (0-31) 5 bits hour (0-23) 6 bits
		 * minute (0-59) 6 bits second (0-59) = (5 bytes in total) +
		 * fractional-seconds storage (size depends on meta)
		 */
		long datetime = bigEndianLong(ByteUtils.readSpecifiedLengthBytes(msg, 5)
		// inputStream.read(5)
		, 0, 5);
		{//修复bug:针对DateTimeV2
			if(549755813888L==datetime){//0000-00-00 00:00:00
				return "0000-00-00 00:00:00";				
			}
		}
		int yearMonth = extractBits(datetime, 1, 17, 40);
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, yearMonth / 13);
		c.set(Calendar.MONTH, yearMonth % 13 - 1);
		c.set(Calendar.DAY_OF_MONTH, extractBits(datetime, 18, 5, 40));
		c.set(Calendar.HOUR_OF_DAY, extractBits(datetime, 23, 5, 40));
		c.set(Calendar.MINUTE, extractBits(datetime, 28, 6, 40));
		c.set(Calendar.SECOND, extractBits(datetime, 34, 6, 40));
		c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, msg));
		return localDateFormat.get().format(c.getTime());
	}

	private static int getFractionalSeconds(int meta, ByteBuf msg) throws IOException {
		int fractionalSecondsStorageSize = getFractionalSecondsStorageSize(meta);
		if (fractionalSecondsStorageSize > 0) {
			long fractionalSeconds = bigEndianLong(ByteUtils.readSpecifiedLengthBytes(msg, fractionalSecondsStorageSize)
			// inputStream.read(fractionalSecondsStorageSize)
			, 0, fractionalSecondsStorageSize);
			if (meta % 2 == 1) {
				fractionalSeconds /= 10;
			}
			return (int) (fractionalSeconds / 1000);
		}
		return 0;
	}

	private static int getFractionalSecondsStorageSize(int fsp) {
		switch (fsp) {
		case 1:
		case 2:
			return 1;
		case 3:
		case 4:
			return 2;
		case 5:
		case 6:
			return 3;
		default:
			return 0;
		}
	}

	/**
	 * see mysql/strings/decimal.c
	 */
	public static long bigEndianLong(byte[] bytes, int offset, int length) {
		long result = 0;
		for (int i = offset; i < (offset + length); i++) {
			byte b = bytes[i];
			result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
		}
		return result;
	}

	private static java.sql.Time deserializeTimeV2(int meta, ByteBuf msg) throws IOException {
		/*
		 * in big endian:
		 * 
		 * 1 bit sign (1= non-negative, 0= negative) 1 bit unused (reserved for
		 * future extensions) 10 bits hour (0-838) 6 bits minute (0-59) 6 bits
		 * second (0-59) = (3 bytes in total) + fractional-seconds storage (size
		 * depends on meta)
		 */
		long time = bigEndianLong(ByteUtils.readSpecifiedLengthBytes(msg, 3)
		// inputStream.read(3)
		, 0, 3);
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(Calendar.HOUR_OF_DAY, extractBits(time, 2, 10, 24));
		c.set(Calendar.MINUTE, extractBits(time, 12, 6, 24));
		c.set(Calendar.SECOND, extractBits(time, 18, 6, 24));
		c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, msg));
		return new java.sql.Time(c.getTimeInMillis());
	}

	private static java.sql.Timestamp deserializeTimestamp(ByteBuf inputStream) throws IOException {
		long value = ByteUtils.readUnsignedLong(inputStream, 4);
		// inputStream.readLong(4);
		return new java.sql.Timestamp(value * 1000L);
	}

	public static java.sql.Timestamp deserializeTimestampV2(int meta, ByteBuf inputStream) throws IOException {
		// big endian
		long timestamp = bigEndianLong(ByteUtils.readSpecifiedLengthBytes(inputStream, 4)
		// inputStream.read(4)
		, 0, 4);
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(timestamp * 1000);
		c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
		return new java.sql.Timestamp(c.getTimeInMillis());
	}

	public static java.util.Date deserializeDatetime(ByteBuf inputStream) throws IOException {
		long value = ByteUtils.readUnsignedLong(inputStream, 8);
		// inputStream.readLong(8);
		int[] split = split(value, 100, 6);
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, split[5]);
		c.set(Calendar.MONTH, split[4] - 1);
		c.set(Calendar.DAY_OF_MONTH, split[3]);
		c.set(Calendar.HOUR_OF_DAY, split[2]);
		c.set(Calendar.MINUTE, split[1]);
		c.set(Calendar.SECOND, split[0]);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	public static Serializable deserializeCell(ColumnType type, int meta, int length, ByteBuf msg) throws IOException {
		// LoggerUtils.debug(logger, "enter deserializeCell");
		// LoggerUtils.debug(logger, "type:" + type);
		switch (type) {
		case BIT:
			int bitSetLength = (meta >> 8) * 8 + (meta & 0xFF);
			return ByteUtils.readBitSet(msg, bitSetLength, false);
		case TINY:
			return (int) ((byte) ByteUtils.readUnsignedByte(msg)
			// inputStream.readInteger(1)
			);
		case SHORT:
			return (int) ((short) ByteUtils.readUnsignedInt(msg, 2)
			// inputStream.readInteger(2)
			);
		case INT24:
			return (ByteUtils.readUnsignedInt(msg, 3)
			// inputStream.readInteger(3)
			<< 8) >> 8;
		case LONG:
			return ByteUtils.readUnsignedInt(msg, 4);
		// inputStream.readInteger(4);
		case LONGLONG:
			return ByteUtils.readUnsignedLong(msg, 8);
		// inputStream.readLong(8);
		case FLOAT:
			return Float.intBitsToFloat(ByteUtils.readUnsignedInt(msg, 4)
			// inputStream.readInteger(4)
			);
		case DOUBLE:
			return Double.longBitsToDouble(ByteUtils.readUnsignedLong(msg, 8)
			// inputStream.readLong(8)
			);
		case NEWDECIMAL:
			int precision = meta & 0xFF, scale = meta >> 8, decimalLength = determineDecimalLength(precision, scale);
			return ByteUtils.toDecimal(precision, scale, ByteUtils.readSpecifiedLengthBytes(msg, decimalLength)
			// inputStream.read(decimalLength)
			);
		case DATE:
			return ByteUtils.deserializeDate(msg);
		case TIME:
			return deserializeTime(msg);
		case TIME_V2:
			return deserializeTimeV2(meta, msg);
		case TIMESTAMP:
			return deserializeTimestamp(msg);
		case TIMESTAMP_V2:
			return deserializeTimestampV2(meta, msg);
		case DATETIME:
			// LoggerUtils.debug(logger, "DATETIME type");
			return deserializeDatetime(msg);
		case DATETIME_V2:
			return deserializeDatetimeV2(meta, msg);
		case YEAR:
			return 1900 + ByteUtils.readUnsignedInt(msg, 1);
		// inputStream.readInteger(1);
		case STRING:
			int stringLength = length < 256 ? ByteUtils.readUnsignedInt(msg, 1)
					// inputStream.readInteger(1)
					: ByteUtils.readUnsignedInt(msg, 2)
					// inputStream.readInteger(2)
					;
			return ByteUtils.readSpecifiedLengthString(msg, stringLength);
		// inputStream.readString(stringLength);
		case VARCHAR:
		case VAR_STRING:
			// LoggerUtils.debug(logger, "meta:" + meta);
			int varcharLength = meta < 256 ? ByteUtils.readUnsignedByte(msg)
					// inputStream.readInteger(1)
					: ByteUtils.readUnsignedInt(msg, 2);
			// inputStream.readInteger(2);
			// LoggerUtils.debug(logger, "varcharLength:" + varcharLength);
			return ByteUtils.readSpecifiedLengthString(msg, varcharLength);
		// return inputStream.readString(varcharLength);
		case BLOB:
			int blobLength = ByteUtils.readUnsignedInt(msg, meta);
			// inputStream.readInteger(meta);
			//return ByteUtils.readSpecifiedLengthBytes(msg, blobLength);
			return ByteUtils.readSpecifiedLengthString(msg, blobLength);
		// return inputStream.read(blobLength);
		case ENUM:
			return ByteUtils.readUnsignedInt(msg, length);
		// inputStream.readInteger(length);
		case SET:
			return ByteUtils.readUnsignedLong(msg, length);
		// inputStream.readLong(length);
		default:
			throw new IOException("Unsupported type " + type);
		}
	}

	private static int extractBits(long value, int bitOffset, int numberOfBits, int payloadSize) {
		long result = value >> payloadSize - (bitOffset + numberOfBits);
		return (int) (result & ((1 << numberOfBits) - 1));
	}

	public static Serializable[] deserializeRow(TableMapEventData tableMapEventData, BitSet includedColumns,
			ByteBuf msg) throws IOException {
		// LoggerUtils.debug(logger, "enter deserializeRow...");
		// LoggerUtils.debug(logger, "deserializeRow1...");
		byte[] types = tableMapEventData.getColumnTypes();
		int[] metadata = tableMapEventData.getColumnMetadata();
		Serializable[] result = new Serializable[numberOfBitsSet(includedColumns)];
		BitSet nullColumns = ByteUtils.readBitSet(msg, result.length, true);
		// LoggerUtils.debug(logger, "deserializeRow2...");
		for (int i = 0, numberOfSkippedColumns = 0; i < types.length; i++) {
			if (!includedColumns.get(i)) {
				numberOfSkippedColumns++;
				continue;
			}
			int index = i - numberOfSkippedColumns;
			if (!nullColumns.get(index)) {
				// mysql-5.6.24 sql/log_event.cc log_event_print_value (line
				// 1980)
				int typeCode = types[i] & 0xFF, meta = metadata[i], length = 0;
				if (typeCode == ColumnType.STRING.getCode()) {
					if (meta >= 256) {
						int meta0 = meta >> 8, meta1 = meta & 0xFF;
						if ((meta0 & 0x30) != 0x30) {
							typeCode = meta0 | 0x30;
							length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
						} else {
							// mysql-5.6.24 sql/rpl_utility.h enum_field_types
							// (line 278)
							if (meta0 == ColumnType.ENUM.getCode() || meta0 == ColumnType.SET.getCode()) {
								typeCode = meta0;
							}
							length = meta1;
						}
					} else {
						length = meta;
					}
				}
				// LoggerUtils.debug(logger, "deserializeRow3...");
				// LoggerUtils.debug(logger, "length..." + length);
				result[index] = deserializeCell(ColumnType.byCode(typeCode), meta, length, msg);
				// LoggerUtils.debug(logger, "leave deserializeCell");
			}
		}
		// LoggerUtils.debug(logger, "leave deserializeRow...");
		return result;
	}

	private static int determineDecimalLength(int precision, int scale) {
		int x = precision - scale;
		int ipDigits = x / DIG_PER_DEC;
		int fpDigits = scale / DIG_PER_DEC;
		int ipDigitsX = x - ipDigits * DIG_PER_DEC;
		int fpDigitsX = scale - fpDigits * DIG_PER_DEC;
		return (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX] + (fpDigits << 2) + DIG_TO_BYTES[fpDigitsX];
	}

	private static BigDecimal toDecimal(int precision, int scale, byte[] value) {
		boolean positive = (value[0] & 0x80) == 0x80;
		value[0] ^= 0x80;
		if (!positive) {
			for (int i = 0; i < value.length; i++) {
				value[i] ^= 0xFF;
			}
		}
		int x = precision - scale;
		int ipDigits = x / DIG_PER_DEC;
		int ipDigitsX = x - ipDigits * DIG_PER_DEC;
		int ipSize = (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX];
		int offset = DIG_TO_BYTES[ipDigitsX];
		BigDecimal ip = offset > 0 ? BigDecimal.valueOf(ByteUtils.bigEndianInteger(value, 0, offset)) : BigDecimal.ZERO;
		for (; offset < ipSize; offset += 4) {
			int i = ByteUtils.bigEndianInteger(value, offset, 4);
			ip = ip.movePointRight(DIG_PER_DEC).add(BigDecimal.valueOf(i));
		}
		int shift = 0;
		BigDecimal fp = BigDecimal.ZERO;
		for (; shift + DIG_PER_DEC <= scale; shift += DIG_PER_DEC, offset += 4) {
			int i = ByteUtils.bigEndianInteger(value, offset, 4);
			fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIG_PER_DEC));
		}
		if (shift < scale) {
			int i = ByteUtils.bigEndianInteger(value, offset, DIG_TO_BYTES[scale - shift]);
			fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
		}
		BigDecimal result = ip.add(fp);
		return positive ? result : result.negate();
	}

	public static int numberOfBitsSet(BitSet bitSet) {
		int result = 0;
		for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
			result++;
		}
		return result;
	}

	private static int[] split(long value, int divider, int length) {
		int[] result = new int[length];
		for (int i = 0; i < length - 1; i++) {
			result[i] = (int) (value % divider);
			value /= divider;
		}
		result[length - 1] = (int) value;
		return result;
	}

	public static java.sql.Time deserializeTime(ByteBuf msg) throws IOException {
		int value = ByteUtils.readUnsignedInt(msg, 3);
		// inputStream.readInteger(3);
		int[] split = split(value, 100, 3);
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(Calendar.HOUR_OF_DAY, split[2]);
		c.set(Calendar.MINUTE, split[1]);
		c.set(Calendar.SECOND, split[0]);
		return new java.sql.Time(c.getTimeInMillis());
	}

	public static java.sql.Date deserializeDate(ByteBuf msg) throws IOException {
		int value = ByteUtils.readUnsignedInt(msg, 3);
		// inputStream.readInteger(3);
		int day = value % 32;
		value >>>= 5;
		int month = value % 16;
		int year = value >> 4;
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 1);
		cal.set(Calendar.DATE, day);
		return new java.sql.Date(cal.getTimeInMillis());
	}

	private static byte[] reverse(byte[] bytes) {
		for (int i = 0, length = bytes.length >> 1; i < length; i++) {
			int j = bytes.length - 1 - i;
			byte t = bytes[i];
			bytes[i] = bytes[j];
			bytes[j] = t;
		}
		return bytes;
	}

	public static BitSet readBitSet(ByteBuf buf, int length, boolean bigEndian) throws IOException {
		// according to MySQL internals the amount of storage required for N
		// columns is INT((N+7)/8) bytes
		byte[] bytes = ByteUtils.readSpecifiedLengthBytes(buf, (length + 7) >> 3);
		bytes = bigEndian ? bytes : reverse(bytes);
		BitSet result = new BitSet();
		for (int i = 0; i < length; i++) {
			if ((bytes[i >> 3] & (1 << (i % 8))) != 0) {
				result.set(i);
			}
		}
		return result;
	}

	public static int bigEndianInteger(byte[] bytes, int offset, int length) {
		int result = 0;
		for (int i = offset; i < (offset + length); i++) {
			byte b = bytes[i];
			result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
		}
		return result;
	}

	public static int[] readMetadata(ByteBuf msg, byte[] columnTypes) throws IOException {
		int[] metadata = new int[columnTypes.length];
		for (int i = 0; i < columnTypes.length; i++) {
			switch (ColumnType.byCode(columnTypes[i] & 0xFF)) {
			case FLOAT:
			case DOUBLE:
			case BLOB:
				metadata[i] = ByteUtils.readUnsignedByte(msg);// inputStream.readInteger(1);
				break;
			case BIT:
			case VARCHAR:
			case NEWDECIMAL:
				metadata[i] = ByteUtils.readUnsignedInt(msg, 2);// inputStream.readInteger(2);
				break;
			case SET:
			case ENUM:
			case STRING:
				metadata[i] = bigEndianInteger(ByteUtils.readSpecifiedLengthBytes(msg, 2), 0, 2);
				break;
			case TIME_V2:
			case DATETIME_V2:
			case TIMESTAMP_V2:
				metadata[i] = ByteUtils.readUnsignedByte(msg);
				// inputStream.readInteger(1);
				// // fsp (@see {@link
				// ColumnType})
				break;
			default:
				metadata[i] = 0;
			}
		}
		return metadata;
	}

	// public static byte readByte(ByteBuf src) {
	// return (byte) src.readUnsignedByte();
	// }

	public static short readUnsignedByte(ByteBuf src) {
		return src.readUnsignedByte();
	}

	// public static int readInt(ByteBuf src, int bits) {
	// int result = 0;
	// for (int i = 0; i < bits; ++i) {
	// result |= (src.readByte() << (i << 3));
	// }
	// return result;
	// }

	public static int readUnsignedInt(ByteBuf src, int bits) {
		int result = 0;
		for (int i = 0; i < bits; ++i) {
			result |= (src.readUnsignedByte() << (i << 3));
		}
		return result;
	}

	// public static long readLong(ByteBuf src, int bits) {
	// long result = 0;
	// for (int i = 0; i < bits; ++i) {
	// result |= (src.readByte() << (i << 3));
	// }
	// return result;
	// }

	public static long readUnsignedLong(ByteBuf src, int bits) {
		long result = 0;
		for (int i = 0; i < bits; ++i) {
			result |= (src.readUnsignedByte() << (i << 3));
		}
		return result;
	}

	public static String readZeroTerminatedString(ByteBuf src) {

		src.markReaderIndex();// 先标记
		int length = 0;
		while ('\0' != src.readByte()) {// 可见字符
			// 一直循环
			length++;
		}
		length++;// 这个长度包含\0
		// 找到了\0 的长度为length
		src.resetReaderIndex();// 恢复
		byte[] str = new byte[length];
		src.readBytes(str);
		// 之前有个bug,
		return new String(str, 0, length - 1);
	}

	public static String readSpecifiedLengthString(ByteBuf src, int length) {

		byte[] str = new byte[length];
		src.readBytes(str);
		return new String(str, 0, length);
	}

	public static byte[] readSpecifiedLengthBytes(ByteBuf src, int length) {

		byte[] str = new byte[length];
		src.readBytes(str);
		return str;
	}

	/**
	 * Format (first-byte-based):<br/>
	 * 0-250 - The first byte is the number (in the range 0-250). No additional
	 * bytes are used.<br/>
	 * 251 - SQL NULL value<br/>
	 * 252 - Two more bytes are used. The number is in the range 251-0xffff.
	 * <br/>
	 * 253 - Three more bytes are used. The number is in the range
	 * 0xffff-0xffffff.<br/>
	 * 254 - Eight more bytes are used. The number is in the range
	 * 0xffffff-0xffffffffffffffff.
	 */
	public static Number readVariableNumber(ByteBuf value) {
		// 读取变长的整数
		int b = ByteUtils.readUnsignedByte(value);
		if (b < 251) {
			return b;
		} else if (b == 251) {
			return null;
		} else if (b == 252) {
			return ByteUtils.readUnsignedLong(value, 2);
		} else if (b == 253) {
			return ByteUtils.readUnsignedLong(value, 3);
		} else if (b == 254) {
			return ByteUtils.readUnsignedLong(value, 8);
		}
		return null;
	}

	// 下面的函数主要用来写
	public static byte[] writeByte(byte value, int length) {
		byte[] result = new byte[length];
		for (int i = 0; i < length; i++) {
			// 小端模式
			result[i] = (byte) (0x000000FF & (value >>> (i << 3)));
		}
		return result;
	}

	public static byte[] writeInt(int value, int length) {
		byte[] result = new byte[length];
		for (int i = 0; i < length; i++) {
			// 小端模式
			result[i] = (byte) (0x000000FF & (value >>> (i << 3)));
		}
		return result;
	}

	public static byte[] writeLong(long value, int length) {
		byte[] result = new byte[length];
		for (int i = 0; i < length; i++) {
			// 小端模式
			result[i] = (byte) (0x000000FF & (value >>> (i << 3)));
		}
		return result;
	}

	public static byte[] writeString(String value) {// 最后要以\0结尾
		value += "\0";
		return value.getBytes();
	}

	// 借鉴了mysql-binlog的代码
	/**
	 * see mysql/sql/password.c scramble(...)
	 */
	public static byte[] passwordCompatibleWithMySQL411(String password, String salt) {
		MessageDigest sha;
		try {
			sha = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		byte[] passwordHash = sha.digest(password.getBytes());
		return xor(passwordHash, sha.digest(union(salt.getBytes(), sha.digest(passwordHash))));
	}

	public static byte[] union(byte[] a, byte[] b) {
		byte[] r = new byte[a.length + b.length];
		System.arraycopy(a, 0, r, 0, a.length);
		System.arraycopy(b, 0, r, a.length, b.length);
		return r;
	}

	public static byte[] xor(byte[] a, byte[] b) {
		byte[] r = new byte[a.length];
		for (int i = 0; i < r.length; i++) {
			r[i] = (byte) (a[i] ^ b[i]);
		}
		return r;
	}

	public static int availableWithChecksumLength(ByteBuf src, int checksumLength) {

		return src.readableBytes() - checksumLength;
	}

}
