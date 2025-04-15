package org.sunbird.dp.eventupdater.domain


import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.sunbird.dp.eventupdater.task.EventUpdaterConfig


object Event {
  def getValueOrDefault[T](value: T, defaultValue: T): T = if (value == null) defaultValue
  else value
}

class Event() {
}

