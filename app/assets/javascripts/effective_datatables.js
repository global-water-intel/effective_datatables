//= require vendor/jquery.delayedChange
//= require vendor/jquery.fileDownload

//= require dataTables/jquery.dataTables
//= require dataTables/dataTables.bootstrap4

//= require dataTables/buttons/dataTables.buttons
//= require dataTables/buttons/buttons.bootstrap4
//= require dataTables/buttons/buttons.colVis
//= require dataTables/buttons/buttons.html5
//= require dataTables/buttons/buttons.print
//= require dataTables/responsive/dataTables.responsive
//= require dataTables/responsive/responsive.bootstrap4
//= require dataTables/rowReorder/dataTables.rowReorder
//= require dataTables/rowReorder/rowReorder.bootstrap4

//= require_tree ./effective_datatables

$.extend( $.fn.dataTable.defaults, {
  'dom': "<'row'<'col-sm-4'l><'col-sm-8'B>><'row'<'col-sm-12'tr>><'row'<'col-sm-6'i><'col-sm-6'p>>"
});
