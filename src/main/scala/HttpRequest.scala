package com.gastecka.demo

import java.io.IOException
import okhttp3.{Headers, OkHttpClient, Request, Response}

class HttpRequest {

  def ExecuteHttpGet(url: String) : Option[String] = {

    val client: OkHttpClient = new OkHttpClient();

    val headerBuilder = new Headers.Builder
    val headers = headerBuilder
      .add("content-type", "application/json")
      .build

    val result = try {
        val request = new Request.Builder()
          .url(url)
          .headers(headers)
          .build();

        val response: Response = client.newCall(request).execute()
        response.body().string()
      }
      catch {
        case _: Throwable => null
      }

    Option[String](result)
  }

}
