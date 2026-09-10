/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.server

import java.nio.file.{Files, Path}
import javax.annotation.Nullable

import com.linecorp.armeria.common.{
  HttpData,
  HttpHeaderNames,
  HttpRequest,
  HttpResponse,
  HttpStatus,
  MediaType,
  ResponseHeaders
}
import com.linecorp.armeria.server.Server
import com.linecorp.armeria.server.annotation.{Get, Param}
import com.linecorp.armeria.server.file.HttpFile
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.MessageTypeParser

import io.delta.sharing.server.common.JsonUtils

/**
 * A small test-only server for exercising CDF reads from a shared view. It also exposes a control
 * table response to verify that `versionlessCDF` is not copied from the request onto table
 * responses.
 */
private[server] object TestViewSharingServer {
  val PORT = 12346
  val ENDPOINT = s"http://localhost:$PORT/delta-sharing"

  private[server] val FILE_PATH = "/view-cdf.parquet"

  def start(port: Int = PORT): Server = {
    val parquetFile = createParquetFile()
    val builder = Server.builder().http(port)
    builder.annotatedService()
      .pathPrefix("/delta-sharing")
      .build(new TestViewSharingService(parquetFile))
    builder.service(FILE_PATH, HttpFile.of(parquetFile).asService())
    val server = builder.build()
    server.start().get()
    server
  }

  private def createParquetFile(): Path = {
    val file = Files.createTempFile("delta-sharing-view-cdf", ".parquet")
    Files.delete(file)
    file.toFile.deleteOnExit()

    val schema = MessageTypeParser.parseMessageType(
      """message view_cdf {
        |  required binary value (UTF8);
        |  required binary _change_type (UTF8);
        |}
        |""".stripMargin)
    val writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(file.toUri))
      .withConf(new Configuration())
      .withType(schema)
      .build()
    val groupFactory = new SimpleGroupFactory(schema)
    try {
      writer.write(groupFactory.newGroup()
        .append("value", "first")
        .append("_change_type", "insert"))
      writer.write(groupFactory.newGroup()
        .append("value", "second")
        .append("_change_type", "delete"))
    } finally {
      writer.close()
    }
    file
  }
}

private[server] class TestViewSharingService(parquetFile: Path) {
  import TestViewSharingService._

  @Get("/shares/view_share/schemas/default/tables/{table}/metadata")
  def getMetadata(req: HttpRequest, @Param("table") table: String): HttpResponse = {
    if (table != VIEW_NAME && table != TABLE_NAME) {
      return errorResponse(HttpStatus.NOT_FOUND, s"Unknown test object: $table")
    }

    val responseFormat = getResponseFormat(req)
    val actions = metadataActions(
      responseFormat,
      isView = table == VIEW_NAME,
      Files.size(parquetFile))
    val headers = ResponseHeaders.builder(HttpStatus.OK)
      .set(HttpHeaderNames.CONTENT_TYPE, DeltaSharingService.DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(
        DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER,
        s"${DeltaSharingService.DELTA_SHARING_RESPONSE_FORMAT}=$responseFormat")
      .set(DeltaSharingService.DELTA_TABLE_VERSION_HEADER, TABLE_VERSION.toString)
      .build()
    HttpResponse.of(headers, HttpData.ofUtf8(actions.mkString("\n") + "\n"))
  }

  @Get("/shares/view_share/schemas/default/tables/{table}/changes")
  def listCdfFiles(
      req: HttpRequest,
      @Param("table") table: String,
      @Param("startingVersion") @Nullable startingVersion: String,
      @Param("endingVersion") @Nullable endingVersion: String,
      @Param("startingTimestamp") @Nullable startingTimestamp: String,
      @Param("endingTimestamp") @Nullable endingTimestamp: String): HttpResponse = {
    val isVersionlessCDF = table == VIEW_NAME
    if (!isVersionlessCDF && table != TABLE_NAME) {
      return errorResponse(HttpStatus.NOT_FOUND, s"Unknown test object: $table")
    }
    if (startingTimestamp == null) {
      return errorResponse(HttpStatus.BAD_REQUEST, "A starting timestamp is required")
    }
    if (isVersionlessCDF && (startingVersion != null || endingVersion != null)) {
      return errorResponse(HttpStatus.BAD_REQUEST, "View CDF only accepts timestamp bounds")
    }

    val requestCapabilities = capabilitiesMap(
      req.headers().get(DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER))
    if (isVersionlessCDF && !requestCapabilities.get(VERSIONLESS_CDF).contains("true")) {
      return errorResponse(HttpStatus.BAD_REQUEST, "The client does not support versionless CDF")
    }

    val responseFormat = getResponseFormat(requestCapabilities)
    val fileUrl = s"http://${req.authority()}${TestViewSharingServer.FILE_PATH}"
    val actions = responseActions(
      responseFormat,
      isVersionlessCDF,
      fileUrl,
      Files.size(parquetFile))

    val responseCapabilities = Seq(
      Some(s"${DeltaSharingService.DELTA_SHARING_RESPONSE_FORMAT}=$responseFormat"),
      if (isVersionlessCDF) Some(s"$VERSIONLESS_CDF_RESPONSE=true") else None
    ).flatten.mkString(DeltaSharingService.DELTA_SHARING_CAPABILITIES_DELIMITER)
    val headers = ResponseHeaders.builder(HttpStatus.OK)
      .set(HttpHeaderNames.CONTENT_TYPE, DeltaSharingService.DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER, responseCapabilities)
    if (!isVersionlessCDF) {
      headers.set(DeltaSharingService.DELTA_TABLE_VERSION_HEADER, TABLE_VERSION.toString)
    }
    HttpResponse.of(headers.build(), HttpData.ofUtf8(actions.mkString("\n") + "\n"))
  }

  private def errorResponse(status: HttpStatus, message: String): HttpResponse = {
    HttpResponse.of(status, MediaType.PLAIN_TEXT_UTF_8, message)
  }

  private def getResponseFormat(req: HttpRequest): String = {
    getResponseFormat(capabilitiesMap(
      req.headers().get(DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER)))
  }

  private def getResponseFormat(requestCapabilities: Map[String, String]): String = {
    requestCapabilities
      .get(DeltaSharingService.DELTA_SHARING_RESPONSE_FORMAT)
      .filter(_.split(",").contains(DELTA_FORMAT))
      .map(_ => DELTA_FORMAT)
      .getOrElse(PARQUET_FORMAT)
  }
}

private[server] object TestViewSharingService {
  val VIEW_NAME = "view"
  val TABLE_NAME = "table"

  private val VERSIONLESS_CDF = "versionlesscdf"
  private val VERSIONLESS_CDF_RESPONSE = "versionlessCDF"
  private val DELTA_FORMAT = "delta"
  private val PARQUET_FORMAT = "parquet"
  private val TABLE_VERSION = 1L
  private val COMMIT_TIMESTAMP = 1652140800000L
  private val SCHEMA =
    """{"type":"struct","fields":[{"name":"value","type":"string","nullable":true,""" +
      """"metadata":{}}]}"""

  private def capabilitiesMap(header: String): Map[String, String] = {
    Option(header).toSeq.flatMap(_.toLowerCase.split(";"))
      .map(_.split("=", 2))
      .collect { case Array(key, value) => key -> value }
      .toMap
  }

  private def responseActions(
      responseFormat: String,
      isVersionlessCDF: Boolean,
      fileUrl: String,
      fileSize: Long): Seq[String] = {
    val metadata = metadataAction(isView = isVersionlessCDF, fileSize)
    val version = if (isVersionlessCDF) Map.empty[String, Any] else Map("version" -> TABLE_VERSION)

    if (responseFormat == DELTA_FORMAT) {
      Seq(
        Map("protocol" -> Map(
          "deltaProtocol" -> Map("minReaderVersion" -> 1, "minWriterVersion" -> 2))),
        Map("metaData" -> (Map("deltaMetadata" -> metadata) ++ version)),
        Map("file" -> (Map[String, Any](
          "id" -> s"$responseFormat-${if (isVersionlessCDF) VIEW_NAME else TABLE_NAME}-change",
          "timestamp" -> COMMIT_TIMESTAMP,
          "deltaSingleAction" -> Map("cdc" -> Map(
            "path" -> fileUrl,
            "partitionValues" -> Map.empty[String, String],
            "size" -> fileSize))) ++ version))
      ).map(JsonUtils.toJson(_))
    } else {
      Seq(
        Map("protocol" -> Map("minReaderVersion" -> 1)),
        Map("metaData" -> metadata),
        Map("cdf" -> (Map[String, Any](
          "url" -> fileUrl,
          "id" -> s"$responseFormat-${if (isVersionlessCDF) VIEW_NAME else TABLE_NAME}-change",
          "partitionValues" -> Map.empty[String, String],
          "size" -> fileSize,
          "timestamp" -> COMMIT_TIMESTAMP) ++ version))
      ).map(JsonUtils.toJson(_))
    }
  }

  private def metadataActions(
      responseFormat: String,
      isView: Boolean,
      fileSize: Long): Seq[String] = {
    val metadata = metadataAction(isView, fileSize)
    if (responseFormat == DELTA_FORMAT) {
      Seq(
        Map("protocol" -> Map(
          "deltaProtocol" -> Map("minReaderVersion" -> 1, "minWriterVersion" -> 2))),
        Map("metaData" -> Map("deltaMetadata" -> metadata))
      ).map(JsonUtils.toJson(_))
    } else {
      Seq(
        Map("protocol" -> Map("minReaderVersion" -> 1)),
        Map("metaData" -> metadata)
      ).map(JsonUtils.toJson(_))
    }
  }

  private def metadataAction(isView: Boolean, fileSize: Long): Map[String, Any] = {
    Map(
      "id" -> (if (isView) "view-id" else "table-id"),
      "format" -> Map("provider" -> "parquet"),
      "schemaString" -> SCHEMA,
      "partitionColumns" -> Seq.empty[String],
      "size" -> fileSize,
      "numFiles" -> 1)
  }
}
