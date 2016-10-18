resetFilters = ->
  window.location.pathname = window.location.pathname + '/reset_filters'
resetAll = ->
  window.location.pathname = window.location.pathname + '/reset_all'

initializeDataTables = ->
  $('table.effective-datatable').each ->
    return if $.fn.DataTable.fnIsDataTable(this)

    datatable = $(this)
    simple = (datatable.data('simple') == true)
    resetFilterButtons = (datatable.data('reset-filter-buttons') == true)
    baseButtons = [
      {
        extend: 'colvis',
        text: 'Show / hide columns',
        postfixButtons: [
          { extend: 'colvisGroup', text: 'Show all', show: ':hidden'},
          { extend: 'colvisRestore', text: 'Show default'}
        ]
      },
      {
        extend: 'copy',
        exportOptions:
          format:
            header: (str) -> $("<div>#{str}</div>").children('.filter-label').first().text()
          columns: ':not(.col-actions)'
      },
      {
        extend: 'csv',
        exportOptions:
          format:
            header: (str) -> $("<div>#{str}</div>").children('.filter-label').first().text()
          columns: ':not(.col-actions)'
      },
      {
        extend: 'excel',
        exportOptions:
          format:
            header: (str) -> $("<div>#{str}</div>").children('.filter-label').first().text()
          columns: ':not(.col-actions)'
      },
      {
        extend: 'print',
        exportOptions:
          format:
            header: (str) -> $("<div>#{str}</div>").children('.filter-label').first().text()
          columns: ':visible:not(.col-actions)'
      },
    ]

    if resetFilterButtons
      extraButtons = [
        {
          text: 'Reset Filters',
          action: resetFilters
        },
        {
          text: 'Reset Everything',
          action: resetAll
        },
      ]
    else
      extraButtons = []
    input_js_options = datatable.data('input-js-options') || {}

    if input_js_options['buttons'] == false
      input_js_options['buttons'] = []

    init_options =
      dom: "<'row'<'col-sm-4'l><'col-sm-8'B>><'row'<'col-sm-12'tr>><'row'<'col-sm-6'i><'col-sm-6'p>>"
      displayStart: datatable.data('display-start')
      pageLength: datatable.data('page-length')
      ajax: { url: datatable.data('source'), type: 'POST' }
      autoWidth: false
      buttons: extraButtons.concat(baseButtons)
      colReorder: false
      # colReorder: !simple
      columns: datatable.data('columns')
      deferLoading: [datatable.data('display-records'), datatable.data('total-records')]
      deferRender: true
      iDisplayLength: datatable.data('display-entries')
      language: { 'lengthMenu': 'Show _MENU_ per page'}
      lengthMenu: [[5, 10, 25, 50, 100, 250, 1000, -1], ['5', '10', '25', '50', '100', '250', '1000', 'All']]
      order: datatable.data('default-order')
      processing: true
      responsive: false
      serverParams: (params) ->
        table = this.api()
        table.columns().flatten().each (index) =>
          params['columns'][index]['visible'] = table.column(index).visible()
      serverSide: true
      scrollCollapse: true
      pagingType: 'simple_numbers'
      initComplete: (settings) ->
        initializeBulkActions(this.api())
        initializeFilters(this.api())
      drawCallback: (settings) ->
        $table = $(this.api().table().node())
        selected = $table.data('bulk-actions-restore-selected-values')
        completeBulkAction($table, selected) if selected && selected.length > 0

        if settings['json']
          if settings['json']['aggregates']
            drawAggregates($table, settings['json']['aggregates'])

          if settings['json']['charts']
            drawCharts($table, settings['json']['charts'])

    # Copies the bulk actions html, stored in a data attribute on the table, into the buttons area
    initializeBulkActions = (api) ->
      $table = $(api.table().node())
      bulkActions = $table.data('bulk-actions')

      if bulkActions
        $table.closest('.dataTables_wrapper').children().first()
          .find('.dt-buttons').first().prepend(bulkActions['dropdownHtml'])

    # After we perform a bulk action, we have to re-select the checkboxes manually and do a bit of house keeping
    completeBulkAction = ($table, selected) ->
      $table.find("input[data-role='bulk-actions-resource']").each (_, input) ->
        $input = $(input)
        $input.prop('checked', selected.indexOf($input.val()) > -1)

      $wrapper = $table.closest('.dataTables_wrapper')
      $wrapper.children().first().find('.buttons-bulk-actions').children('button').removeAttr('disabled')
      $table.siblings('.dataTables_processing').html('Processing...')

    drawAggregates = ($table, aggregates) ->
      $tfoot = $table.find('tfoot').first()

      $.each aggregates, (row, values) =>
        $row = $tfoot.children().eq(row)

        if $row
          $.each values, (col, value) => $row.children().eq(col).html(value)

    drawCharts = ($table, charts) ->
      $.each charts, (name, data) =>
        $(".effective-datatables-chart[data-name='#{name}']").each (_, obj) =>
          chart = new google.visualization[data['type']](obj)
          chart.draw(google.visualization.arrayToDataTable(data['data']), data['options'])

    # Appends the filter html, stored in the column definitions, into each column header
    initializeFilters = (api) ->
      api.columns().flatten().each (index) =>
        $th = $(api.column(index).header())
        settings = api.settings()[0].aoColumns[index] # column specific settings

        if settings.filterSelectedValue  # Assign preselected filter values
          api.settings()[0].aoPreSearchCols[index].sSearch = settings.filterSelectedValue

        if settings.filterHtml  # Append the html filter HTML and initialize input events
          $th.append('<br>' + settings.filterHtml)
          if !$th.hasClass('col-bulk_actions_column')
            api.column(index).search($th.find('input,select').val())
          initializeFilterEvents($th)

    # Sets up the proper events for each input
    initializeFilterEvents = ($th) ->
      $th.find('input,select').each (_, input) ->
        $input = $(input)

        return true if $input.attr('type') == 'hidden' || $input.attr('type') == 'checkbox'

        $input.parent().on 'click', (event) -> false # Dont order columns when you click inside the input
        $input.parent().on 'mousedown', (event) -> event.stopPropagation() # Dont order columns when you click inside the input

        if $input.is('select')
          $input.on 'change', (event) -> dataTableSearch($(event.currentTarget))
        else if $input.is('input')
          delayedChangeOptions =
            onChange: ($input) -> dataTableSearch($input)
            oldValue: $input.val()
          $input.delayedChange delayedChangeOptions

    # Do the actual search
    dataTableSearch = ($input) ->   # This is the function called by a select or input to run the search
      table = $input.closest('table.dataTable')
      table.DataTable().column("#{$input.data('column-name')}:name").search($input.val()).draw()

    if simple
      init_options['dom'] = "<'row'<'col-sm-12'tr>>" # Just show the table
      datatable.addClass('sort-hidden')

    # Let's actually initialize the table now
    table = datatable.dataTable(jQuery.extend(init_options, input_js_options))

    # Apply EffectiveFormInputs to the Show x per page dropdown
    if datatable.data('effective-form-inputs')
      try table.closest('.dataTables_wrapper').find('.dataTables_length select').select2()

    # Capture column visibility changes and refresh datatable
    datatable.on 'column-visibility.dt', (event, settings, index, state) ->
      $table = $(event.currentTarget)

      timeout = $table.data('timeout')
      clearTimeout(timeout) if timeout
      $table.data('timeout', setTimeout( =>
          $table.DataTable().draw()
          $.event.trigger('turbolinks:load')
        , 700)
      )

destroyDataTables = ->
  $('table.effective-datatable').each ->
    if $.fn.DataTable.fnIsDataTable(this)
      $(this).DataTable().destroy()

$ -> initializeDataTables()
$(document).on 'turbolinks:load', -> initializeDataTables()
$(document).on 'effective-datatables:initialize', -> initializeDataTables()
$(document).on 'turbolinks:before-cache', -> destroyDataTables()

