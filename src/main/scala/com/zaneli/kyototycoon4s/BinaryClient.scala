package com.zaneli.kyototycoon4s

import com.zaneli.kyototycoon4s.binary.{BinaryProtocolEncoder, BinaryProtocolDecoder}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpClientCodec}

class BinaryClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  def test(): Unit = {
    val group = new NioEventLoopGroup()
    try {
      val bootstrap = new Bootstrap()
      bootstrap.group(group)
        .channel(classOf[NioSocketChannel])
        .remoteAddress(host, port)
        .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
        .handler(new BinaryProtocoChannelInitializer())
      val future = bootstrap.connect().sync()
      future.channel().closeFuture().sync()
      println("closed")
    } finally {
      group.shutdownGracefully()
    }
  }
}

class BinaryProtocolHandler() extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("channelActive")
    ctx.writeAndFlush(Seq("test"))
  }
}

class BinaryProtocoChannelInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline
    pipeline.addLast(new BinaryProtocolEncoder())
    pipeline.addLast(new BinaryProtocolDecoder())
    pipeline.addLast(new HttpClientCodec())
    pipeline.addLast(new HttpObjectAggregator(65536))
    pipeline.addLast(new BinaryProtocolHandler())
  }
}
